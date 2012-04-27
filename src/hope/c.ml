open Lwt
open Common
open Modules
    
module ProtocolHandler (S:Core.STORE) = struct      
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
	
	let get_test_and_set_args ic =
	  Llio.input_string ic >>= fun k ->
	  Llio.input_string_option ic >>= fun m_old ->
	  Llio.input_string_option ic >>= fun m_new ->
	  Lwt.return (k, m_old, m_new)
	
	let get_key ic =
	  Llio.input_string ic
	    
	let send_string_option oc so =
	  Llio.output_int oc 0 >>= fun () ->
	  Llio.output_string_option oc so 
	  
	let __do_unit_update driver q =
	  DRIVER.push_cli_req driver q >>= fun a ->
	  match a with 
	    | Core.UNIT -> Lwt.return ()
	    | Core.FAILURE (rc, msg) -> failwith msg
	    | Core.VALUE v -> failwith "Expected unit, not value"
	      
	let _set driver k v = 
	  let q = Core.SET(k,v) in
	  __do_unit_update driver q
	
	let _sequence driver sequence = __do_unit_update driver sequence
	    
	let _delete driver k =
	  let q = Core.DELETE k in
	  __do_unit_update driver q
	
	let wrap_not_found f d k =
	  Lwt.catch 
	    (fun () -> f d k >>= fun k -> Lwt.return (Some k))
	    (function 
	      | Not_found -> Lwt.return None
	      | e -> Lwt.fail e
	    )
	
  let _safe_get = S.get 
  
	let _get store k = 
    _safe_get store k >>= function
      | None -> Lwt.fail (Common.XException(Arakoon_exc.E_NOT_FOUND, k))
      | Some v -> Lwt.return v 
	
	let _safe_get = wrap_not_found _get 
	
  let am_i_master store = true
  
	let _range store ~(allow_dirty:bool) 
	    (first:string option) (finc:bool) 
	    (last:string option)  (linc:bool)
	    (max:int) = 
    if allow_dirty || am_i_master store 
    then
	    S.range store first finc last linc max
    else
      failwith "Cannot perform range on not-master"
	
	let _get_meta store = S.get_meta store 
	
	let _last_entries store i oc = S.last_entries store (Core.TICK i) oc
	
	let one_command driver store ((ic,oc) as conn) = 
	  Client_protocol.read_command conn >>= fun comm ->
	  match comm with
	    | WHO_MASTER ->
	      Lwtc.log "who master" >>= fun () ->
	      _get_meta store >>= fun ms ->
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
	            _get store key >>= fun value ->
	            Client_protocol.response_rc_string oc 0l value)
	          (Client_protocol.handle_exception oc)
	      end 
	    | LAST_ENTRIES ->
	      begin
	        Sn.input_sn ic >>= fun i ->
	        Llio.output_int32 oc 0l >>= fun () ->
	        _last_entries store i oc >>= fun () ->
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
	    | MULTI_GET ->
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
	            Lwt_list.map_s (fun k -> _get store k ) keys >>= fun values ->
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
	        Llio.input_bool ic >>= fun finc  ->
	        Llio.input_string_option ic >>= fun (last:string option)  ->
	        Llio.input_bool ic >>= fun linc  ->
	        Llio.input_int ic >>= fun max   ->
	        Lwt.catch
	        (fun () ->
	          _range store ~allow_dirty first finc last linc max >>= fun list ->
	          Llio.output_int32 oc 0l >>= fun () ->
	          Llio.output_list Llio.output_string oc list >>= fun () ->
	          Lwt.return false
		      )
	        (Client_protocol.handle_exception oc )
	      end
	    | CONFIRM ->
	      begin
	        Llio.input_string ic >>= fun key ->
	        Llio.input_string ic >>= fun value ->
	        _safe_get store key >>= fun v ->
	        if v <> Some value 
	        then
	          Lwt.catch
	          (fun () -> 
	            _set driver key value >>= fun () ->
	            Client_protocol.response_ok_unit oc)
	          (Client_protocol.handle_exception oc)
	        else 
	          Client_protocol.response_ok_unit oc
	      end
	    | TEST_AND_SET ->
	      get_test_and_set_args ic >>= fun (key, m_old, m_new) ->
	      _safe_get store key >>= fun m_val ->
	      begin
	        if m_val = m_old 
	        then
	          begin
	            match m_new with
	              | None -> Lwtc.log "Test_and_set: delete" >>= fun () -> _delete driver key
	              | Some v -> Lwtc.log "Test_and_set: set" >>= fun () ->  _set driver key v
	          end 
	        else
	          begin
	            Lwtc.log "Test_and_set: nothing to be done"
	          end
	      end >>= fun () ->
	      send_string_option oc m_val >>= fun () ->
	      Lwt.return false
	    | DELETE ->
	      get_key ic >>= fun key ->
	      _delete driver key >>= fun () ->
	      Client_protocol.response_ok_unit oc
	    | _ -> Client_protocol.handle_exception oc (Failure "Command not implemented (yet)")
	      
	let protocol driver store (ic,oc) =   
	  let rec loop () = 
	    begin
	      one_command driver store (ic,oc) >>= fun stop ->
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
	
end
