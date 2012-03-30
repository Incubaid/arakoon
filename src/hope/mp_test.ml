
open Mp
open MULTI
open Mp_driver
open Mp_test_dispatch
open MPTestDispatch
open Lwt 
open Pq
open Core

module SMDriver = MPDriver(MPTestDispatch)
open SMDriver

let test_max_n = 256
let test_max_i = 256

let _log m = Lwt_io.printf m

let id x = x

let (>>+) m f = List.fold_left (fun a b -> List.rev_append a (f b)) [] m 

(*  List.flatten( List.rev_map f m )  *)

(* 
  let g acc x = (f x) :: acc in
  List.fold_left g [] m 
*)


let ret x = [x] 
  
let range n = 
  let rec range_helper = function
    | 0 -> []
    | i -> (n-i) :: range_helper (i-1)
  in 
  range_helper n 
  
let gen_accepted_from_proposed proposed =
  begin
    let tx, prop =
      begin 
        if Random.bool()
        then
           let r_value = Printf.sprintf "%d_%d" (Random.int 128) (Random.int 128) in
           prev_tick , Some (Core.SET ("AWAITING_CONSENSUS", r_value))
        else
           id, None
      end
    in
    let s =
      begin
        match Random.int 4 with
          | 0 -> S_CLUELESS
          | 1 -> S_MASTER
          | 2 -> S_SLAVE
          | _ -> S_RUNNING_FOR_MASTER
      end
    in
    let ch = 
      begin
        match s, prop with
          | S_MASTER, Some x -> ch_node
          | _ -> ch_all
      end
    in
    (tx proposed), s, prop, ch
  end


let test_cnt = ref 0

let gen_master c = function
  | S_RUNNING_FOR_MASTER
  | S_MASTER              -> Some c.me
  | S_CLUELESS            -> None
  | S_SLAVE               -> 
    let ix = Random.int (c.node_cnt - 1) in
    Some (List.nth c.others ix) 
 
let rec get_random_sublist = function
  | [] -> []
  | hd :: tl ->
    begin
      if Random.bool() 
      then get_random_sublist tl
      else  hd :: (get_random_sublist tl)
    end
     
let gen_votes s_n m_prop c =
  begin
    let v () = get_random_sublist (c.me :: c.others) in    
    match s_n, m_prop with
      | S_RUNNING_FOR_MASTER, _ -> v ()
      | S_MASTER, Some x -> v ()
      | _, _ -> []
  end
    
let build_random_state c =
  let gen_n = TICK( Random.int test_max_n ) in
  let gen_i = TICK( Random.int test_max_i ) in
  let (gen_accepted, gen_state_n, gen_prop, gen_inputs) = 
       gen_accepted_from_proposed gen_i 
  in
  let gen_master_id = gen_master c gen_state_n in
  let gen_v = gen_votes gen_state_n gen_prop c in
  let gen_now = 0.0 in
  let gen_lease_expiration = 
       gen_now +. Random.float (2.0 *. c.lease_duration) 
  in
  let gen_waiting_client = None in
  {
    round             = gen_n;
    proposed          = gen_i;
    accepted          = gen_accepted;
    state_n           = gen_state_n;
    master_id         = gen_master_id;
    prop              = gen_prop;
    votes             = gen_v;
    now               = gen_now;
    lease_expiration  = gen_lease_expiration;
    constants         = c;
    cur_cli_req       = gen_waiting_client;
    valid_inputs      = gen_inputs;
  }


let multi_driver_serve drivers_with_states step_cnt =
  let can_driver_step (d,s) =
    let nr, cr = SMDriver.is_ready d in
    match s.valid_inputs with
      | l when l = ch_all  -> nr || cr
      | _ -> nr
  in
  let split_timeouts now (left, right) (n, d) =
    begin
      if d <= now 
      then
        ( n::left, right )
      else
        ( left, (n,d)::right )
    end
  in
  let rec add_timeouts now dr_w_state =
    let rec add_timeouts_helper now acc = function
      | [] -> acc
      | (dr,s) :: tl ->
        let l, r = List.fold_left (split_timeouts now) ([],[]) dr.action_dispatcher.timeouts in
        List.iter (fun n -> SMDriver.push_msg dr (M_LEASE_TIMEOUT n)) l;
        dr.action_dispatcher.timeouts <- r;
        let new_acc =  acc || ( List.length l > 0 ) in
        add_timeouts_helper now new_acc tl
    in
    begin
      if add_timeouts_helper now false dr_w_state
      then
        now
      else
        let now' = now +. 3.0 in
        add_timeouts now' dr_w_state
    end
  in
  let rec pick_first now acc = function
    | [] -> 
      let now' = add_timeouts now acc in
      pick_first now' [] acc
    | hd :: tl -> 
      begin
        if can_driver_step hd
        then
          hd, tl @ acc, now
        else
          pick_first now (hd::acc) tl
      end
  in
  let rec do_step drivers_with_states = function
    | 0 -> _log "All done with multi_driver_serve\n"
    | i -> 
      let now = float_of_int (step_cnt - i) in 
      let (rdy_dr, rdy_st), rest, now = pick_first now [] drivers_with_states in
      SMDriver.step rdy_dr rdy_st now >>= fun new_s ->
      let () = MPTestDispatch.update_state rdy_dr.action_dispatcher new_s in
      let new_drivers_with_states = rest @ [(rdy_dr, new_s)] in
      do_step new_drivers_with_states  (i - 1)
  in
  let () = List.iter (fun (d,s) -> MPTestDispatch.update_state d.action_dispatcher s ) drivers_with_states in
  do_step drivers_with_states step_cnt  

let build_constants nodes lease =
  let node_cnt = List.length nodes in
  let node_ixs = range(node_cnt) in
  let q = (List.length nodes / 2) + 1 in
  let build_constants_inner (i, n) =
    let n_others = List.filter ((<>) n) nodes in 
    {
      quorum = q;
      me = n;
      others = n_others;
      lease_duration = lease;
      node_ix = i;
      node_cnt = node_cnt;
    } 
  in
  List.map build_constants_inner (List.combine node_ixs nodes)
    
let test_iteration nodes lease step_cnt p_cs states =
    let dispatches = List.map (fun s -> MPTestDispatch.create()) states in
    let drivers = List.map (fun disp -> SMDriver.create disp ) dispatches in
    let named_drivers = List.combine nodes drivers in
    let drivers_and_state = List.combine drivers states in
    let add_all_msg_targets disp =
      List.iter (fun(n, driver) -> MPTestDispatch.add_msg_target disp n driver.SMDriver.msgs) named_drivers
    in
    let () = List.iter (fun disp -> add_all_msg_targets disp) dispatches in
    let ts' = List.fold_left 
         (fun acc (driver, state) ->
           let timeout = M_LEASE_TIMEOUT state.round in 
           (Lwt.return( SMDriver.push_msg driver timeout)) :: acc ) 
         [] drivers_and_state 
    in
    let waiters = List.map (fun driver -> SMDriver.push_cli_req driver (Core.DELETE "key")) drivers in
    let ts'' = List.fold_left (fun acc w -> (w >>= fun r -> Lwt.return()) :: acc) ts' waiters in
    
    Lwt.join ( (multi_driver_serve (List.combine drivers states) step_cnt) :: ts'')

let test_list_bind () =
  let l = ( [1;2;3] >>+ fun i ->  [5;6;7] >>+ fun n -> ret (i,n)) in
  Lwt.return ()
  (* Lwt_list.iter_s (fun (i,n) -> _log "%d %d\n" i n ) l *)
     

let run_test_cases node1 node2 node3 lease =
  let nodes = [node1; node2; node3] in
  let n  = TICK 100 in
  let i  = TICK 100 in
  let pi    = prev_tick i in
  let ppi   = prev_tick pi in
  let ni    = next_tick i in
  let nni   = next_tick ni in
  let upds  = Hashtbl.create 12 in
  Hashtbl.replace upds ppi (DELETE (tick2s ppi));
  Hashtbl.replace upds pi (DELETE (tick2s pi));
  Hashtbl.replace upds i (DELETE (tick2s i));
  Hashtbl.replace upds ni (DELETE (tick2s ni));
  Hashtbl.replace upds nni (DELETE (tick2s nni));  
  let pis   = [pi; i] in
  let ais   = [i; prev_tick i] in
  let cs    = build_constants nodes lease in
  let ss    = [S_MASTER; S_SLAVE; S_CLUELESS; S_RUNNING_FOR_MASTER] in
  let p1 = i in
  let u1 = Hashtbl.find upds p1 in
  let c1 = List.nth cs 0 in
  let c2 = List.nth cs 1 in
  let c3 = List.nth cs 2 in
  let n1 = n in
  let n2s = [prev_n n1 3 1; n1 ; next_n n1 3 1] in
  let vs = [ []; [node1] ; [node2] ; nodes ] in
  let step_cnt = 500 in
  let timer () = 
    Lwt_unix.sleep(20.0)
  in
  let get_master_from_state s me other =
    begin
      match s with
        | S_CLUELESS -> None
        | S_SLAVE -> Some other
        | S_MASTER
        | S_RUNNING_FOR_MASTER -> Some me
    end
  in
  let get_votes s =
    begin
      if s = S_RUNNING_FOR_MASTER || s = S_MASTER  
      then vs 
      else [[]]
    end
  in
  let get_channels s = 
    begin
      if s = S_MASTER then [ch_all; ch_node] else [ch_all]
    end
  in
  begin
    ais >>+ fun a1 ->
    pis >>+ fun p2 ->
    let u2 = Hashtbl.find upds p2 in
    [p2; prev_tick p2] >>+ fun a2 ->
    [prev_tick p2; p2] >>+ fun p3 -> 
    let u3 = Hashtbl.find upds p3 in
    [p3; prev_tick p3] >>+ fun a3 -> 
    ss >>+ fun s1 ->
    ss >>+ fun s2 ->
    ss >>+ fun s3 -> 
    let m1 = get_master_from_state s1 node1 node2 in
    let m2 = get_master_from_state s2 node2 node1 in
    let m3 = get_master_from_state s3 node3 node1 in
    n2s >>+ fun n2 ->
    [prev_n n2 3 2; n2; next_n n2 3 2] >>+ fun n3 ->
    (get_votes s1) >>+ fun v1 ->
    (get_votes s2) >>+ fun v2 ->
    (get_votes s3) >>+ fun v3 -> 
    (get_channels s1) >>+ fun ch1 ->
    (get_channels s2) >>+ fun ch2 ->
    (get_channels s3) >>+ fun ch3 ->
    let now1 = 100.0 in
    let now2 = now1 in
    let now3 = now1 in
    [ now1 -. lease ; now1 ; now1 +. lease ] >>+ fun le1 ->
    [ now2 -. lease ; now2 ; now2 +. lease ] >>+ fun le2 ->
    [ now3 -. lease ; now3 ; now3 +. lease ] >>+ fun le3 ->
    
    test_cnt := !test_cnt + 1;
    (*
    Lwt.catch 
    (fun () ->
      let s1 = {
        round             = n1;
        proposed          = p1;
        accepted          = a1;
        state_n           = s1;
        master_id         = m1;
        prop              = Some u1;
        votes             = v1;
        now               = now1;
        lease_expiration  = le1;
        constants         = c1;
        cur_cli_req       = None;
        valid_inputs      = ch1;
      } in
      let s2 = {
        round             = n2;
        proposed          = p2;
        accepted          = a2;
        state_n           = s2;
        master_id         = m2;
        prop              = Some u2;
        votes             = v2;
        now               = now2;
        lease_expiration  = le2;
        constants         = c2;
        cur_cli_req       = None;
        valid_inputs      = ch2;
      } in
      let s3 = {
        round             = n3;
        proposed          = p3;
        accepted          = a3;
        state_n           = s3;
        master_id         = m3;
        prop              = Some u3;
        votes             = v3;
        now               = now3;
        lease_expiration  = le3;
        constants         = c3;
        cur_cli_req       = None;
        valid_inputs      = ch3;
      } in 
      Lwt.pick( [ timer() ; test_iteration nodes lease step_cnt cs [s1;s2;s3]] )
    ) 
    (fun e -> 
        Lwt.return ( print_string (Printexc.to_string e) ) 
    ) >>= fun () ->
    Lwt.return 
    *) 
    ret () 
      
  end
  
let main() =
  let s  = run_test_cases "n1" "n2" "n3" 10.0 in
  _log "combination count : %d\n" !test_cnt >>= fun () ->
  (*let _ = List.map (fun (s1,s2) -> 
      _log "%s" (state2s s1) >>= fun () ->
      _log "%s" (state2s s2)
      ) s in
  *)
  Lwt.return ()

let old() =
  let () = Random.self_init() in
  let n = ["n1"; "n2"; "n3"] in
  let lease = 20.0 in
  let step_cnt = 500 in
  let timer () = 
    Lwt_unix.sleep(20.0)
  in
  let p_cs = build_constants n lease in
  let states = List.map build_random_state p_cs in
  Lwt.catch 
    (fun () ->
        Lwt.pick( [ timer() ; test_iteration n lease step_cnt p_cs states] )
    ) 
    (fun e -> 
        Lwt.return ( print_string (Printexc.to_string e) ) 
    ) 


let () = Lwt_main.run( main() )

