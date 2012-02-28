open Mem_store
open Hub
open Lwt

module MHub = HUB(MemStore)

let gen_request_id =
  let c = ref 0 in
  fun () -> let r = !c in 
            let () = incr c in 
            r

module C = struct
  open Common
  type t = TODO

  let prologue (ic,oc) = 
    let check magic version = 
      if magic = _MAGIC && 
        version = _VERSION 
      then Lwt.return ()
      else Llio.lwt_failfmt "MAGIC %lx or VERSION %x mismatch" magic version
    in
    let check_cluster cluster_id = 
      let ok = true in
      if ok then Lwt.return ()
      else Llio.lwt_failfmt "WRONG CLUSTER: %s" cluster_id
    in
    Llio.input_int32  ic >>= fun magic ->
    Llio.input_int    ic >>= fun version ->
    check magic version  >>= fun () ->
    Llio.input_string ic >>= fun cluster_id ->
    check_cluster cluster_id >>= fun () ->
    Lwt.return ()
    
  let __do_unit_update hub q =
    let id = gen_request_id () in
    MHub.update hub id q >>= fun a ->
    match a with 
      | Core.UNIT -> Lwt.return ()

  let _set hub k v = 
    let q = Core.SET(k,v) in
    __do_unit_update hub q

  let _delete hub k =
    let id = gen_request_id () in
    let q = Core.DELETE k in
    MHub.update hub id q >>= fun a ->
    match a with 
      | Core.UNIT -> Lwt.return ()

  let _get hub k = MHub.get hub k


  let one_command hub ((ic,oc) as conn) = 
    Client_protocol.read_command conn >>= fun comm ->
    match comm with
      | WHO_MASTER ->
        _log "who master" >>= fun () ->
        let mo = Some "arakoon_0" in
        Llio.output_int32 oc 0l >>= fun () ->
        Llio.output_string_option oc mo >>= fun () ->
        Lwt.return false
      | SET -> 
        begin
          Llio.input_string ic >>= fun key ->
          Llio.input_string ic >>= fun value ->
          _log "set %S %S" key value >>= fun () ->
          Lwt.catch
            (fun () -> 
              _set hub key value >>= fun () ->
              Client_protocol.response_ok_unit oc)
            (Client_protocol.handle_exception oc)
        end
      | GET ->
        begin
          Llio.input_bool ic >>= fun allow_dirty ->
          Llio.input_string ic >>= fun key ->
          _log "get %S" key >>= fun () ->
          Lwt.catch 
            (fun () -> 
              _get hub key >>= fun value ->
              Client_protocol.response_rc_string oc 0l value)
            (Client_protocol.handle_exception oc)
        end 

  let protocol hub (ic,oc) =   
    _log "session started" >>= fun () ->
    let rec loop () = 
      begin
        one_command hub (ic,oc) >>= fun stop ->
        if stop
        then _log "end of session"
        else 
          begin
            Lwt_io.flush oc >>= fun () ->
            loop ()
          end
      end
    in
    prologue(ic,oc) >>= fun () -> 
    _log "prologue ok" >>= fun () ->
    loop ()

end

let server_t hub =
  let host = "127.0.0.1" 
  and port = 4000 in
  let inner = Server.make_server_thread host port (C.protocol hub) in
  inner ()

let main_t () =
  let hub = MHub.create () in
  let service hub = server_t hub in
  Lwt.join [ MHub.serve hub;
             service hub
           ];;

let () =  Lwt_main.run (main_t())
  
