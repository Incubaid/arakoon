
open Mp
open MULTI
open Lwt
open Core
open Pq

module ADispatcher (S:STORE)  = struct

  type t =
  {
    store : S.t; 
    msg : Messaging.messaging;
    timeout_q : message PQ.q;
  }
  
  let get t k =
    S.get t.store k 
    
  let create msging s tq=  
  {
    store = s;
    msg = msging;  
    timeout_q = tq;
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
  
  let store_lease store m e =
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
      send_msg t me tgt msg_str >>= fun () ->
      Lwt.return s
    | A_COMMIT_UPDATE (i, u, m_w) ->
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
      log_update t i u >>= fun res ->
      let s' = {
        s with
        proposed = i
      } in
      Lwt.return s'
    | A_START_TIMER (n, d) ->
      let alarm () =
        Lwt_unix.sleep d >>= fun () ->
        let msg = M_LEASE_TIMEOUT n in
        PQ.push t.timeout_q msg;
        Lwt.return ()
      in
      Lwt.ignore_result( alarm() ); 
      Lwt.return s
    | A_CLIENT_REPLY (w, r) ->
      safe_wakeup w r >>= fun () ->
      Lwt.return s
    | A_STORE_LEASE (m, e) ->
      store_lease t.store m e >>= fun () ->
      Lwt.return 
        { s with
          lease_expiration = e;
          master_id = Some m;
        }
    
    | _ -> 
      failwith "Unknown action type"


end