open Lwt
open Common
open Modules
    
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
    
let __do_unit_update driver q =
  DRIVER.push_cli_req driver q >>= fun a ->
  match a with 
    | Core.UNIT -> Lwt.return ()
    | Core.FAILURE (rc, msg) -> failwith msg
      
let _set driver k v = 
  let q = Core.SET(k,v) in
  __do_unit_update driver q
    
let _delete driver k =
  let q = Core.DELETE k in
  __do_unit_update driver q
    
let _get driver k = DRIVER.get driver k
  
let _get_meta driver = DRIVER.get_meta driver 
  
let one_command driver ((ic,oc) as conn) = 
  Client_protocol.read_command conn >>= fun comm ->
  match comm with
    | WHO_MASTER ->
      _log "who master" >>= fun () ->
      _get_meta driver >>= fun ms ->
      let mo =
        begin 
          match ms with
            | None -> None
            | Some s -> 
              begin
                let m, off = Llio.string_from s 0 in
                let e, off = Llio.float_from s off in
                if e > ( Unix.gettimeofday() ) 
                then
                  Some m
                else 
                  None 
              end
        end
        in
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
            _set driver key value >>= fun () ->
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
            _get driver key >>= fun value ->
            Client_protocol.response_rc_string oc 0l value)
          (Client_protocol.handle_exception oc)
      end 
        
let protocol driver (ic,oc) =   
  let rec loop () = 
    begin
      one_command driver (ic,oc) >>= fun stop ->
      if stop
      then _log "end of session"
      else 
        begin
          Lwt_io.flush oc >>= fun () ->
          loop ()
        end
    end
  in
  _log "session started" >>= fun () ->
  prologue(ic,oc) >>= fun () -> 
  _log "prologue ok" >>= fun () ->
  loop ()

