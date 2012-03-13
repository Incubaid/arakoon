
open Mem_store
open Mp_driver
open Mp
open Lwt
open Pq

open MULTI

let _log f =  Printf.kprintf Lwt_io.printl f 

let range n = 
  let rec range_helper = function
    | 0 -> []
    | i -> (n-i) :: range_helper (i-1)
  in 
  range_helper n 

module MPDispatch = struct 

  type t = {
    msging : (string, message PQ.q ) Hashtbl.t ;
    store : MemStore.t;
  }

  let add_msg_target t id q =
    Hashtbl.replace t.msging id q
    
  let create() = 
  {
    msging = Hashtbl.create 3;
    store = MemStore.create();
  }
  
  let send_msg t dest msg =
     let q = Hashtbl.find t.msging dest in
     PQ.push q msg 

  let dispatch t s = function
    | A_DIE msg -> Lwt.fail( Failure msg )
    | A_BROADCAST_MSG msg ->  
    
      _log " - ACTION: Broadcast %s" (msg2s msg)  >>= fun () ->
      let send_msg id q = 
        PQ.push q msg
      in 
      let () = Hashtbl.iter send_msg t.msging in
      Lwt.return s 
    | A_SEND_MSG (msg, dest) ->
      _log " - ACTION: Send %s" (msg2s msg) >>= fun () ->
      let () = send_msg t dest msg in
      Lwt.return s
    | A_RESYNC (n, i, tgt) ->
      _log " - ACTION: Resync from %s: n=%s i=%s" tgt (tick2s n) (tick2s i) >>= fun () ->
      Lwt.return s
    | A_START_TIMER (n, d) ->
      _log " - ACTION: Start timer n=%s d=%f" (tick2s n) d >>= fun () ->
      Lwt.ignore_result (
        Lwt_unix.sleep d >>= fun () ->
        let msg = M_LEASE_TIMEOUT n in
        Lwt.return ( send_msg t s.constants.me msg ) 
      ) ;
      Lwt.return s
    | A_COMMIT_UPDATE (i, u, m_w) -> 
      _log " - ACTION: Commit update %s: %s" (tick2s i) (Core.update2s u) >>= fun () ->
      begin
        match m_w with
          | None -> Lwt.return ()
          | Some w -> Lwt.return (Lwt.wakeup w Core.UNIT)
      end >>= fun () ->
      Lwt.return s
    | A_LOG_UPDATE (i, u) ->
      _log " - ACTION: Log update" >>= fun () ->
      Lwt.return s
    | A_CLIENT_REPLY r -> 
      _log " - ACTION: Client reply" >>= fun () ->
      begin
        match r with
          | CLIENT_REPLY_FAILURE (w, rc, msg) ->
              Lwt.wakeup w (Core.FAILURE (rc, msg)) 
      end ;
      Lwt.return s
end

module SMDriver = MPDriver(MPDispatch)
 
let run_lwt () =
  let main () =
    let nodes = ["n1"; "n2"; "n3"] in
    let node_cnt = List.length nodes in
    let node_ixs = range(node_cnt) in
    let lease = 3.0 in
    let q = (List.length nodes / 2) + 1 in
    let build_constants (i, n) =
      {
        quorum = q;
        me = n;
        lease_duration = lease;
        node_ix = i;
        node_cnt = node_cnt;
      } 
    in
    let p_cs = List.map build_constants (List.combine node_ixs nodes) in  
    let build_state c =
	  {
	    round             = start_tick;
	    proposed          = start_tick;
	    accepted          = start_tick;
	    state_n           = S_CLUELESS;
	    master_id         = None;
	    prop              = None;
	    votes             = [] ;
	    now               = Unix.gettimeofday() ;
	    lease_expiration  = 0.0;
	    constants         = c;
	    cur_cli_req       = None;
	    valid_inputs      = ch_all;
	  }
    in
    let states = List.map build_state p_cs in
    let dispatches = List.map (fun s -> MPDispatch.create()) states in
    let drivers = List.map (fun disp -> SMDriver.create disp ) dispatches in
    let named_drivers = List.combine nodes drivers in
    let add_all_msg_targets disp =
      List.iter (fun(n, driver) -> MPDispatch.add_msg_target disp n driver.SMDriver.msgs) named_drivers
    in
    let () = List.iter (fun disp -> add_all_msg_targets disp) dispatches in
    let ts = List.map (fun(d,s) -> SMDriver.serve d s (Some 15)) (List.combine drivers states) in
    let timeout = M_LEASE_TIMEOUT (TICK 0) in
    let ts' = List.fold_left (fun acc driver -> (Lwt.return( SMDriver.push_msg driver timeout)) :: acc ) ts drivers in
    let waiters = List.map (fun driver -> SMDriver.push_cli_req driver (Core.DELETE "key")) drivers in
    let ts'' = List.fold_left (fun acc w -> (w >>= fun r -> Lwt.return()) :: acc) ts' waiters in
    Lwt.join ts''
  in
  Lwt_main.run( main () )

let () = run_lwt ()