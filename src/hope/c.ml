open Lwt
open Modules
open Statistics
open Routing
open Interval
open Client_cfg
open Vcommon




module ProtocolHandler (S:Core.STORE) = struct      
  open Baardskeerder
  module V   = VCommon(S)
  module One = V1.V1(S)
  module Two = V2.V2(S)

  let prologue (ic,oc) = 
    let check magic = 
      if magic = Common._MAGIC 
      then Lwt.return ()
      else Llio.lwt_failfmt "MAGIC %lx mismatch" magic
    in
    let check_cluster cluster_id = 
      let ok = true in
      if ok then Lwt.return ()
      else Llio.lwt_failfmt "WRONG CLUSTER: %s" cluster_id
    in
    Llio.input_int32  ic >>= fun magic ->
    Llio.input_int    ic >>= fun version ->
    check magic          >>= fun () ->
    Llio.input_string ic >>= fun cluster_id ->
    check_cluster cluster_id >>= fun () ->
    Lwt.return version
      

    
  let protocol me (stats:Statistics.t) driver store (ic,oc) =   
    let loop vx_command = 
      let rec _loop () =  
        begin
          vx_command me stats driver store (ic,oc) >>= fun stop ->
          if stop
          then Lwtc.log "end of session: %s" me
          else 
            begin
              Lwt_io.flush oc >>= fun () ->
              _loop ()
            end
        end
      in 
      _loop ()
    in
    Lwtc.log "session started: %s" me >>= fun () ->
    prologue(ic,oc) >>= fun pv ->
    let vx_command = 
      match pv with
        | 2 -> Two.one_command
        | 1 -> One.one_command 
        | _ -> failwith "bad protocol version"
    in
    Lwtc.log "prologue ok: %s" me >>= fun () ->
    loop vx_command
      
end
