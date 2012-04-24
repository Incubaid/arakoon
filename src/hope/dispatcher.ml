
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
    mutable meta : string option;
    resyncs : (node_id, (S.t -> unit Lwt.t)) Hashtbl.t ; 
  }

  let create msging s tq resyncs=  
	  {
	    store = s;
	    msg = msging;
	    timeout_q = tq;
	    meta = None;
	    resyncs = resyncs;
	  } 
    
  let get_meta t = Lwt.return t.meta
    
  let get t k = S.get t.store k 

  let range t first finc last linc max = S.range t.store first finc last linc max

  let last_entries (t:t) (i:Core.tick) (oc:Llio.lwtoc) = S.last_entries t.store i oc

  let create msging s tq resyncs =  
  {
    store = s;
    msg = msging;  
    timeout_q = tq;
    meta = None;
    resyncs = resyncs;
  }
  
  let send_msg t src dst msg =
    t.msg # send_message msg src dst 
  
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
  
  let store_lease t m e =
    let buf = Buffer.create 32 in
    Llio.string_to buf m;
    Llio.float_to buf e;
    let s = Buffer.contents buf in
    Lwt.return (t.meta <- Some s)
  
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
      
  let dispatch t s = function
    | A_BROADCAST_MSG msg ->
      let me = s.constants.me in
      let tgts = me :: s.constants.others in
      let msg_str = string_of_msg msg in
      Lwt_list.iter_p (fun tgt -> send_msg t me tgt msg_str) tgts >>= fun () ->
      Lwt.return s  
    | A_SEND_MSG (msg, tgt) ->
      let me = s.constants.me in
      let msg_str = string_of_msg msg in
      let m = MULTI.msg2s msg in
      Lwtc.log "SENDING %s to %s" m tgt >>= fun () ->
      send_msg t me tgt msg_str >>= fun () ->
      Lwt.return s
    | A_COMMIT_UPDATE (i, u, m_w) ->
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
    | A_LOG_UPDATE (i, u) ->
      let () = validate_log_update i s.proposed in
      let d = (s.proposed <> i) in
      log_update t d u >>= fun res ->
      let s' = {
        s with
        proposed = i
      } in
      Lwt.return s'
    | A_START_TIMER (n, m, d) ->
      start_timer t n m d >>= fun () -> 
      Lwt.return s
    | A_CLIENT_REPLY (w, r) ->
      safe_wakeup w r >>= fun () ->
      Lwt.return s
    | A_STORE_LEASE (m, e) ->
      store_lease t m e >>= fun () ->
      Lwt.return 
        { s with
          lease_expiration = e;
          master_id = Some m;
        }
    | A_RESYNC (tgt, n) -> 
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
        if s.round = n 
        then Lwt.return (s.round, s.extensions) 
        else 
          let n, m = (n, start_tick) in
          start_timer t n start_tick s.constants.lease_duration >>= fun () ->
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

