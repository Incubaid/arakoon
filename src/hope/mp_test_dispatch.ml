open Pq
open Mp
open MULTI
open Mem_store
open Lwt
open Mem_log

let _log f =  Printf.kprintf Lwt_io.printl f 

module MPTestDispatch = struct 
  
  let g_states = Hashtbl.create 3;

  type t = {
    mutable timeouts : (tick * float) list;
    msging   : (string, message PQ.q ) Hashtbl.t ;
    store    : MemStore.t;   
    log      : MemLog.t;
  }

  let send_msg t dest msg =
     let q = Hashtbl.find t.msging dest in
     PQ.push q msg 

  let start_lease t s n d = 
    t.timeouts <- (n, s.now +. d)::t.timeouts
    
  let add_msg_target t id q =
    Hashtbl.replace t.msging id q
    
  let create() = 
  {
    msging = Hashtbl.create 3;
    store = MemStore.create();
    log = MemLog.create();
    timeouts = [];
  }
  
  let update_state t s = 
    Hashtbl.replace g_states s.constants.me s  

  let dispatch t (s, l) = function
    | A_DIE msg -> Lwt.fail( Failure (l ^ msg) )
    | A_BROADCAST_MSG msg ->  
      let log_msg = Printf.sprintf "-> ACTION     : Broadcast %s\n" (msg2s msg) in
      let send_msg id q = 
        PQ.push q msg
      in 
      let () = Hashtbl.iter send_msg t.msging in
      Lwt.return (s, l ^ log_msg)  
    | A_SEND_MSG (msg, dest) ->
      let log_msg = Printf.sprintf "-> ACTION     : Send %s\n" (msg2s msg) in
      let () = send_msg t dest msg in
      Lwt.return (s, l ^ log_msg)
    | A_RESYNC tgt ->
      let log_msg = Printf.sprintf  "-> ACTION     : Resync from %s\n" tgt in
      let s_other = Hashtbl.find g_states tgt in 
      let (n,i) = (s_other.round, s_other.accepted) in 
      let s' = {
        s with
          round = n;
          proposed = i;
          accepted = i;
          votes = [];
          prop = None;
          state_n = S_SLAVE;
          master_id = Some tgt;
      } in
      start_lease t s n (s.constants.lease_duration);
      Lwt.return (s', l ^ log_msg )
    | A_START_TIMER (n, d) ->
      let log_msg = Printf.sprintf "-> ACTION     : Start timer (n: %s) (d: %f)\n" 
           (tick2s n) d in
      start_lease t s n d;
      Lwt.return (s, l ^ log_msg)
    | A_COMMIT_UPDATE (i, u, m_w) -> 
      let log_msg = Printf.sprintf  "-> ACTION     : Commit update (i: %s): %s\n" 
           (tick2s i) (Core.update2s u) in
      begin
        match m_w with
          | None -> Lwt.return ()
          | Some w -> Lwt.return (Lwt.wakeup w Core.UNIT)
      end >>= fun () ->
      let s' = {
        s with
        accepted = i;
      } in
      Lwt.return (s', l ^ log_msg )
    | A_LOG_UPDATE (i, u) ->
      let log_msg = Printf.sprintf "-> ACTION     : Log update (i: %s): %s\n" 
           (tick2s i) (Core.update2s u) in
      let () = MemLog.log_update t.log i u in
      let s' = {
        s with
        proposed = i;
      } in
      Lwt.return (s', l ^ log_msg )
    | A_CLIENT_REPLY r -> 
      let log_msg = "-> ACTION     : Client reply\n" in
      begin
        match r with
          | CLIENT_REPLY_FAILURE (w, rc, msg) ->
              Lwt.wakeup w (Core.FAILURE (rc, msg)) 
      end ;
      Lwt.return (s, l ^ log_msg)
end