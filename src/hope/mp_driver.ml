open Lwt
open Core
open Pq
open Mp

let _log f = Lwt_io.printl f 

module P=MULTI

module MPDriver (A:MP_ACTION_DISPATCHER) = struct
  
  type msg_availability =
    | MSG_RDY
    | CLI_RDY

  type t = {
    msgs : P.message PQ.q;
    reqs : (result Lwt.u * update) PQ.q;
    action_dispatcher : A.t 
  }
  
  let dispatch t a s =
    A.dispatch t.action_dispatcher a s
      
    
  let create dispatcher = { 
    msgs = PQ.create (); 
    reqs = PQ.create (); 
    action_dispatcher = dispatcher;
  }
  
  
  let get_next_msg t s =
    let maybe_msg () =
      (if not (PQ.is_empty t.msgs) then Lwt.return () else PQ.wait_for t.msgs )
      >>= fun () -> Lwt.return (MSG_RDY) 
    in  
    let maybe_client  () =
      begin
        if PQ.is_empty t.msgs 
        then
          (if not (PQ.is_empty t.reqs) then Lwt.return () else PQ.wait_for t.reqs )
          >>= fun () -> Lwt.return CLI_RDY
        else
          Lwt.return MSG_RDY
      end   
    in
    begin
      if List.mem P.CH_CLIENT s.P.valid_inputs 
      then
        Lwt.pick [maybe_msg () ;maybe_client ();]
      else
        Lwt.pick [maybe_msg () ;]
    end 
    >>= fun what ->
    begin
      match what with
        | MSG_RDY -> Lwt.return (PQ.pop t.msgs)
        | CLI_RDY -> 
		      let req_id, upd = PQ.pop t.reqs in
          let m = P.M_CLIENT_REQUEST (req_id, upd) in
          Lwt.return m
    end

  let step t s n =
    get_next_msg t s >>= fun msg ->
    let s = {
      s with 
        P.now = n;
      } in
      let before_msg = Printf.sprintf "BEFORE        : %s\nINPUT         : %s\n" (P.state2s s) (P.msg2s msg) in
      let actions, s' = P.step msg s in
      let after_msg = Printf.sprintf "AFTER STEP    : %s\n" (P.state2s s') in
      Lwt_list.fold_left_s (dispatch t) (s', before_msg ^ after_msg) actions >>= fun (s'', action_log) ->
      let final_state = Printf.sprintf "AFTER ACTIONS : %s\n" (P.state2s s'') in
      _log (action_log ^ final_state ) >>= fun () ->
      Lwt.return s''
       
            
  let serve t s m_step_count =
    Lwt.catch ( 
      fun () ->
        let rec loop s f = function
          | 0 -> _log (Printf.sprintf "%s is all done!!!\n" s.P.constants.P.me)
          | i -> 
            let n = Unix.gettimeofday() in
            step t s n >>= fun s'' -> 
            loop s'' f (f i) 
        in
        let (start_i, f) =
          match m_step_count with
            | None -> 1, (fun i -> i) 
            | Some j -> j, (fun j -> j-1)
        in
        loop s f start_i
    ) ( 
      fun e ->
        _log (Printexc.to_string e) >>= fun () ->
        Lwt.fail e
    ) 
      
  let is_ready t =  not (PQ.is_empty t.msgs), not (PQ.is_empty t.reqs) 
  
  let push_cli_req t upd =
    let waiter, wakener = Lwt.wait() in
    PQ.push t.reqs (wakener, upd);
    waiter
    
  let push_msg t msg =
    PQ.push t.msgs msg
end