
open Mp
open MULTI
open Lwt
open Core
open Pq


let validate_log_update i p =
  if i = p || i = (next_tick p) 
  then ()
  else 
    let msg = Printf.sprintf 
      "Invalid log update request. (i:%s) (p:%s)" (tick2s i) (tick2s p) in
    failwith msg

let validate_commit_update i a =
  if i = (next_tick a) 
  then ()
  else 
    let msg = Printf.sprintf
      "Invalid commit update request. (i:%s) (a:%s)" (tick2s i) (tick2s a) in
    failwith msg
    
    
module ADispatcher (S:STORE) = struct

  type t =
  {
    store : S.t; 
    msg : Messaging.messaging;
    timeout_q : message PQ.q;
    resyncs : (node_id, (S.t -> unit Lwt.t)) Hashtbl.t ; 
  }

  let create msging s tq resyncs=  
	  {
	    store = s;
	    msg = msging;
	    timeout_q = tq;
	    resyncs = resyncs;
	  } 
  
  let log_update t i u = 
    S.log t.store i u
  

  let commit_update t i =
    S.commit t.store i
  
  let safe_wakeup w res =
    Lwt.catch
      (fun () -> Lwt.return(Lwt.wakeup w res))
      (fun e  -> 
        let msg = Printexc.to_string e in
        Lwt_log.error_f "Failed to awaken client (%s). Ignoring." msg
      ) 
      
  let store_lease t mo =
    let buf = Buffer.create 32 in
    Llio.string_option_to buf mo;
    let s = Buffer.contents buf in
    S.set_meta t.store s
  
  let start_timer t n m d =
    Lwtc.log "STARTING TIMER (n: %s) (m: %s) (d: %f)" (tick2s n) (tick2s m) d >>= fun () ->
    let alarm () =
      Lwt_unix.sleep d >>= fun () ->
      let msg = M_LEASE_TIMEOUT (n, m) in
      Lwtc.log "Pushing timeout msg (n: %s) (m: %s)" (tick2s n) (tick2s m) >>= fun () ->
      PQ.push t.timeout_q msg;
      Lwt.return ()
    in
    Lwt.ignore_result( alarm() );
    Lwt.return ()
    
  let handle_commit t s i u m_w =
    let () = validate_commit_update i s.accepted in
    commit_update t i >>= fun res ->
    begin
      match m_w with
        | None -> Lwt.return ()
        | Some w -> safe_wakeup w res
    end
    >>= fun () -> 
    let s' = {
      s with
      accepted = i
    } in
    Lwt.return s'

  let do_send_msg t s msg tgts =
    let me = s.constants.me in
    let msg_str = string_of_msg msg in
    Lwt_list.iter_p (fun tgt -> t.msg # send_message msg_str me tgt ) tgts 
    
  let dispatch t s = function
    | A_BROADCAST_MSG msg ->
      let tgts = s.constants.me :: s.constants.others in
      do_send_msg t s msg tgts >>= fun () ->
      Lwt.return s  
    | A_SEND_MSG (msg, tgt) ->
      do_send_msg t s msg [tgt] >>= fun () ->
      Lwt.return s
    | A_COMMIT_UPDATE (i, u, m_w) ->
      Lwtc.log "Committing update (i:%s)" (tick2s i) >>= fun () ->
      handle_commit t s i u m_w
    | A_LOG_UPDATE (i, u, cli_req) ->
      let () = validate_log_update i s.proposed in
      Lwtc.log "Logging update (i:%s)" (tick2s i) >>= fun () ->
      let d = (s.proposed <> i) in
      begin
	      log_update t d u >>= fun res ->
        let handle_client_failure rc msg = 
          begin
            (* Update log failed *)
            match cli_req with
              | None -> Lwt.return ()
              | Some cli -> safe_wakeup cli (FAILURE (rc, msg)) 
          end
          >>= fun () -> Lwt.return s
        in
        match res with 
          
	        | S.TX_SUCCESS ->
            if s.master_id = Some s.constants.me 
            then
		          begin
				      (* Update logged succesfull *)
				        match s.constants.others with
		              (* If single node, we need to commit as well *)
		 		          | [] ->  
				            let s' = {
				              s with
				              prop = None;
				              proposed = i;
				              votes = [];
				              cur_cli_req = None;
				              valid_inputs = ch_all;
				            } in
	                  handle_commit t s' i u cli_req 
		              (* Not single node, send out accepts *)
				          | others ->
				            let msg = M_ACCEPT(s.constants.me, s.round, s.extensions, i, u) in
	                  do_send_msg t s msg others >>= fun () ->
	                  let s' =  {
				              s with
				              cur_cli_req = cli_req;
				              votes = [s.constants.me];
				              proposed = i;
				              prop = Some u;
				              valid_inputs = ch_node_and_timeout;
				            } in 
	                  Lwt.return s'
    	          end
              else
                begin
                  let s' = { 
                    s with
                    proposed = i;
                    prop = Some u;
                  } in
                  Lwt.return s' 
                end
          | S.TX_ASSERT_FAIL k ->
            handle_client_failure Arakoon_exc.E_ASSERTION_FAILED k
	        | S.TX_NOT_FOUND k -> 
            handle_client_failure Arakoon_exc.E_NOT_FOUND k
		  end
    | A_START_TIMER (n, m, d) ->
      start_timer t n m d >>= fun () -> 
      Lwt.return s
    | A_CLIENT_REPLY (w, r) ->
      safe_wakeup w r >>= fun () ->
      Lwt.return s
    | A_STORE_LEASE mo ->
      store_lease t mo >>= fun () ->
      Lwt.return 
        { s with
          master_id = mo;
        }
    | A_RESYNC (tgt, n, m) -> 
      let resync = Hashtbl.find t.resyncs tgt in
      resync t.store >>= fun () ->
      S.last_update t.store >>= fun m_upd ->
      let i =
        begin 
          match m_upd with
            | None -> start_tick
            | Some (i,_) -> i
        end
      in 
      begin 
        if s.round = n && s.extensions = m
        then Lwt.return (s.round, s.extensions) 
        else 
          start_timer t n m s.constants.lease_duration >>= fun () ->
          Lwt.return (n, m) 
      end >>= fun (n,m) -> 
      let s' = { s with
        round = n;
        extensions = m;
        state_n = S_SLAVE;
        proposed = i;
        prop = None;
        accepted = i;
      } in
      Lwt.return s'

end

