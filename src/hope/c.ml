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


let _sequence driver sequence = __do_unit_update driver sequence
    
let _delete driver k =
  let q = Core.DELETE k in
  __do_unit_update driver q
    
let _get driver k = DRIVER.get driver k

let _range driver ~(allow_dirty:bool) 
    (first:string option) (finc:bool) 
    (last:string option)  (linc:bool)
    (max:int) = 
  DRIVER.range driver ~allow_dirty first finc last linc max

let _get_meta driver = DRIVER.get_meta driver 

let _last_entries driver i oc = DRIVER.last_entries driver (Core.TICK i) oc

let one_command driver ((ic,oc) as conn) = 
  Client_protocol.read_command conn >>= fun comm ->
  match comm with
    | WHO_MASTER ->
      Lwtc.log "who master" >>= fun () ->
      _get_meta driver >>= fun ms ->
      let mo =
        begin 
          match ms with
            | None -> None
            | Some s -> 
              begin
                let m, off = Llio.string_from s 0 in
                Some m 
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
        Lwt.catch 
          (fun () -> 
            _get driver key >>= fun value ->
            Client_protocol.response_rc_string oc 0l value)
          (Client_protocol.handle_exception oc)
      end 
    | LAST_ENTRIES ->
      begin
        Sn.input_sn ic >>= fun i ->
        Llio.output_int32 oc 0l >>= fun () ->
        _last_entries driver i oc >>= fun () ->
        Sn.output_sn oc (Sn.of_int (-1)) >>= fun () ->
        Lwtc.log "end of command" >>= fun () ->
        Lwt.return false
      end
    | SEQUENCE ->
      begin
        Lwt.catch
          (fun () ->
            begin
              Llio.input_string ic >>= fun data ->
              let probably_sequence,_ = Core.update_from data 0 in
              let sequence = match probably_sequence with
                | Core.SEQUENCE _ -> probably_sequence
                | _ -> raise (Failure "should be update")
              in
              _sequence driver sequence >>= fun () ->
              Client_protocol.response_ok_unit oc
            end
          )
          (Client_protocol.handle_exception oc)
      end
    |MULTI_GET ->
      begin
        Llio.input_bool ic >>= fun allow_dirty ->
        Llio.input_int ic >>= fun length ->
        let rec loop keys i = 
          if i = 0 then Lwt.return keys
          else 
            begin
              Llio.input_string ic >>= fun key ->
              loop (key :: keys) (i-1)
            end
        in
        loop [] length >>= fun keys ->
        Lwt.catch 
          (fun () ->
            Lwt_list.map_s (_get driver) keys >>= fun values ->
            Llio.output_int oc 0>>= fun () ->
            Llio.output_int oc length >>= fun () ->
            Lwt_list.iter_s (Llio.output_string oc) values >>= fun () ->
            Lwt.return false
          )
          (Client_protocol.handle_exception oc)
      end
    | RANGE ->
      begin
	Llio.input_bool ic >>= fun allow_dirty ->
        Llio.input_string_option ic >>= fun (first:string option) ->
        Llio.input_bool          ic >>= fun finc  ->
        Llio.input_string_option ic >>= fun (last:string option)  ->
        Llio.input_bool          ic >>= fun linc  ->
        Llio.input_int           ic >>= fun max   ->
        Lwt.catch
	  (fun () ->
	    _range driver ~allow_dirty first finc last linc max >>= fun list ->
	    Llio.output_int32 oc 0l >>= fun () ->
	    Llio.output_list Llio.output_string oc list >>= fun () ->
            Lwt.return false
	  )
	  (Client_protocol.handle_exception oc )
      end
        
let protocol driver (ic,oc) =   
  let rec loop () = 
    begin
      one_command driver (ic,oc) >>= fun stop ->
      if stop
      then Lwtc.log "end of session"
      else 
        begin
          Lwt_io.flush oc >>= fun () ->
          loop ()
        end
    end
  in
  Lwtc.log "session started" >>= fun () ->
  prologue(ic,oc) >>= fun () -> 
  Lwtc.log "prologue ok" >>= fun () ->
  loop ()

