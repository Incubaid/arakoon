open Lwt
open Core
open Pq
open Mp

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
      
  let create dispatcher msg_q reqs_q = { 
    msgs = msg_q; 
    reqs = reqs_q; 
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
        Lwt.pick [maybe_msg (); maybe_client ();]
      else
        Lwt.pick [maybe_msg ();]
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
    let n' =
      begin
        match n with
          | None -> Unix.gettimeofday()
          | Some t -> t
      end
    in
    let s = {
      s with 
        P.now = n';
      } in
    let before_msg = Printf.sprintf "BEFORE        : %s\nINPUT         : %s" (P.state2s s) (P.msg2s msg) in
    let res = P.step msg s in
    begin
      match res with
        | P.StepFailure msg ->
          failwith msg
        | P.StepSuccess (actions, s') ->
          let after_msg = Printf.sprintf "AFTER STEP    : %s" (P.state2s s') in
          Lwtc.log "%s\n%s" before_msg after_msg  >>= fun () ->
          Lwt_list.fold_left_s (dispatch t) s' actions >>= fun s'' ->
          let final_state = Printf.sprintf "AFTER ACTIONS : %s\n" (P.state2s s'') in
          Lwtc.log "%s" final_state >>= fun () ->
          Lwt.return s''
    end
                 
  let serve t s m_step_count =
    Lwt.catch ( 
      fun () ->
        let rec loop s f = function
          | 0 -> Lwtc.log "%s is all done!!!\n" s.P.constants.P.me
          | i -> 
            step t s None >>= fun s'' -> 
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
        Lwtc.log "%s" (Printexc.to_string e) >>= fun () ->
        Lwt.fail e
    ) 
      
  let is_ready t =  not (PQ.is_empty t.msgs), not (PQ.is_empty t.reqs) 
  
  let push_cli_req t upd =
    let waiter, wakener = Lwt.wait() in
    PQ.push t.reqs (wakener, upd);
    waiter
    
  let push_msg t msg =
    PQ.push t.msgs msg
  
  let get t k = A.get t.action_dispatcher k
    
  let get_meta t = A.get_meta t.action_dispatcher
    
  let last_entries t i oc = A.last_entries t.action_dispatcher i oc

  let range t ~allow_dirty first finc last linc max = 
    A.range t.action_dispatcher first finc last linc max

end

