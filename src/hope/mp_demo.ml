
open Mem_store
open Mp_driver
open Mp
open Lwt
open Mp_test_dispatch

open MULTI


let range n = 
  let rec range_helper = function
    | 0 -> []
    | i -> (n-i) :: range_helper (i-1)
  in 
  range_helper n 


module SMDriver = MPDriver(MPTestDispatch)
 
let run_lwt () =
  let main () =
    let nodes = ["n1"; "n2"; "n3"] in
    let node_cnt = List.length nodes in
    let node_ixs = range(node_cnt) in
    let lease = 3.0 in
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
    let dispatches = List.map (fun s -> MPTestDispatch.create()) states in
    let drivers = List.map (fun disp -> SMDriver.create disp ) dispatches in
    let named_drivers = List.combine nodes drivers in
    let add_all_msg_targets disp =
      List.iter (fun(n, driver) -> MPTestDispatch.add_msg_target disp n driver.SMDriver.msgs) named_drivers
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