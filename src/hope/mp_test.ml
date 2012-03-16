
open Mp
open MULTI
open Mp_driver
open Mp_test_dispatch
open Lwt

module SMDriver = MPDriver(MPTestDispatch)

let test_max_n = 256
let test_max_i = 256

let id x = x

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
          | S_MASTER, Some x -> ch_node_and_timeout
          | _ -> ch_all
      end
    in
    (tx proposed), s, prop, ch
  end

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
  let gen_now = Random.float (Unix.gettimeofday()) in
  let gen_lease_expiration = 
       gen_now -. c.lease_duration +. Random.float (2.0 *. c.lease_duration) 
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

let test_iteration nodes lease =
    let node_cnt = List.length nodes in
    let node_ixs = range(node_cnt) in
    let q = (List.length nodes / 2) + 1 in
    let build_constants (i, n) =
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
    let p_cs = List.map build_constants (List.combine node_ixs nodes) in
    let states = List.map build_random_state p_cs in
    let dispatches = List.map (fun s -> MPTestDispatch.create()) states in
    let drivers = List.map (fun disp -> SMDriver.create disp ) dispatches in
    let named_drivers = List.combine nodes drivers in
    let drivers_and_state = List.combine drivers states in
    let add_all_msg_targets disp =
      List.iter (fun(n, driver) -> MPTestDispatch.add_msg_target disp n driver.SMDriver.msgs) named_drivers
    in
    let () = List.iter (fun disp -> add_all_msg_targets disp) dispatches in
    let ts = List.map (fun(d,s) -> SMDriver.serve d s (Some 15)) (List.combine drivers states) in
    let ts' = List.fold_left 
         (fun acc (driver, state) ->
           let timeout = M_LEASE_TIMEOUT state.round in 
           (Lwt.return( SMDriver.push_msg driver timeout)) :: acc ) 
         [] drivers_and_state 
    in
    let waiters = List.map (fun driver -> SMDriver.push_cli_req driver (Core.DELETE "key")) drivers in
    let ts'' = List.fold_left (fun acc w -> (w >>= fun r -> Lwt.return()) :: acc) ts' waiters in
    
    Lwt.join (ts @ ts'')
    
let main() =
  let () = Random.self_init() in
  let n = ["n1"; "n2"; "n3"] in
  let lease = 3.0 in
  let timer () = 
    Lwt_unix.sleep(20.0)
  in
  Lwt.catch 
    (fun () ->
        Lwt.pick( [ timer() ; test_iteration n lease ] )
    ) 
    (fun e -> 
        Lwt.return ( print_string (Printexc.to_string e) ) 
    ) 
  

let () = Lwt_main.run( main() )
  