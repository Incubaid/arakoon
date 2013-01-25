
open Mp
open MULTI
open Lwt
open Core
open Pq

let ntick2s = NTickUtils.tick2s
and mtick2s = MTickUtils.tick2s
and itick2s = ITickUtils.tick2s

let validate_log_update i p =
  if i = p || i = (ITickUtils.next_tick p) 
  then ()
  else 
    let msg = Printf.sprintf 
      "Invalid log update request. (i:%s) (p:%s)" (itick2s i) (itick2s p) in
    failwith msg

let validate_commit_update i a =
  if i = (ITickUtils.next_tick a) 
  then ()
  else 
    let msg = Printf.sprintf
      "Invalid commit update request. (i:%s) (a:%s)" (itick2s i) (itick2s a) in
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
    Lwtc.log "STARTING TIMER (n: %s) (m: %s) (d: %f)" (ntick2s n) (mtick2s m) d >>= fun () ->
    let alarm () =
      Lwt_unix.sleep d >>= fun () ->
      let msg = M_LEASE_TIMEOUT (n, m) in
      Lwtc.log "Pushing timeout msg (n: %s) (m: %s)" (ntick2s n) (mtick2s m) >>= fun () ->
      PQ.push t.timeout_q msg;
      Lwt.return ()
    in
    Lwt.ignore_result( alarm() );
    Lwt.return ()
    
  let handle_commit t s i u m_w to_client =
    let () = validate_commit_update i s.accepted in
    commit_update t i >>= fun () ->
    begin
      match m_w with
        | None -> Lwt.return ()
        | Some w -> safe_wakeup w to_client
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
      Lwtc.log "Committing update (i:%s)" (itick2s i) >>= fun () ->
      handle_commit t s i u m_w Core.VOID
    | A_LOG_UPDATE (i, u, cli_req) ->
      let () = validate_log_update i s.proposed in
      Lwtc.log "Logging update (i:%s)" (itick2s i) >>= fun () ->
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
          
          | Baardskeerder.OK (x: v option) -> (* TODO: put x in state and return upward after consensus *)
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
                    let to_client = 
                      match x with
                        | None -> VOID
                        | Some v -> VALUE v
                    in
                    handle_commit t s' i u cli_req to_client
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
          | Baardskeerder.NOK (rc,k) -> handle_client_failure rc k

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
      begin
      match s.cur_cli_req with
        | None -> Lwt.return ()
        | Some c -> 
          let res = Core.FAILURE(Arakoon_exc.E_NOT_MASTER, "Lost master role") in
          safe_wakeup c res 
      end >>= fun () -> 
      let resync = Hashtbl.find t.resyncs tgt in
      resync t.store >>= fun () ->
      S.last_update t.store >>= fun m_upd ->
      let i =
        begin 
          match m_upd with
            | None -> ITickUtils.start_tick
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
        master_id = Some tgt;
        proposed = i;
        prop = None;
        accepted = i;
        cur_cli_req = None;
      } in
      Lwt.return s'

end

