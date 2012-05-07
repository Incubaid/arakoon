open Lwt
open Modules

let _MAGIC = 0xb1ff0000l
let _MASK  = 0x0000ffffl
let _VERSION = 2

    
let __routing_key = "routing"
let __interval_key = "interval"
    
let my_read_command (ic,oc) = 
  
  let s = 8 in
  let h = String.create s in
  Lwt_io.read_into_exactly ic h 0 s >>= fun () ->
  let hex_string h =
    let r = String.create 24 in
    let char_of_nibble n = 
      let off = if n < 10 then 48 else 55 in
      Char.chr(off + n)
    in
    let rec loop i = 
      if i = 8 
      then r
      else
        let cc = Char.code h.[i] in
        let b0 = (cc land 0xf0) lsr 4 in
        let b1 = (cc land 0x0f) in
        let () = r.[3*i  ] <- char_of_nibble b0 in
        let () = r.[3*i+1] <- char_of_nibble b1 in
        let () = r.[3*i+2] <- ' ' in
        loop (i+1)
    in
    loop 0
  in
  Lwtc.log "my_read_command: %s" (hex_string h) >>= fun () -> 
  let masked,p0 = Llio.int32_from h 4 in
  let magic = Int32.logand masked _MAGIC in
  if magic <> _MAGIC 
  then 
    begin
      Llio.output_int32 oc 1l >>= fun () ->
      Lwtc.failfmt "%08lx has no magic masked" masked
    end
  else
    begin
      let as_int32 = Int32.logand masked _MASK in
      try
        let c = Common.lookup_code as_int32 in
        let size,_   = (Llio.int_from h 0) in
        let rest_size = size -4 in
        let rest = String.create rest_size in
        Lwt_io.read_into_exactly ic rest 0 rest_size >>= fun () ->
        Lwt.return (c, Baardskeerder.Pack.make_input rest 0)
      with Not_found ->
        Llio.output_int32 oc 5l >>= fun () ->
        let msg = Printf.sprintf "%08lx: command not found" as_int32 in
        Llio.output_string oc msg >>= fun () ->
        Lwt.fail (Failure msg)
    end


module ProtocolHandler (S:Core.STORE) = struct      
  open Baardskeerder
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
      
      
  let get_range_params input =
    let allow_dirty = Pack.input_bool input in
    let first = Pack.input_string_option input in 
    let finc = Pack.input_bool input in
    let last = Pack.input_string_option input in
    let linc = Pack.input_bool input in
    let max = Pack.input_option Pack.input_vint input in
    (allow_dirty, first, finc, last, linc, max)
    
  let send_string_option oc so =
    Llio.output_int oc 0 >>= fun () ->
    Llio.output_string_option oc so 
      
  let __do_unit_update driver q =
    DRIVER.push_cli_req driver q >>= fun a ->
    match a with 
      | Core.UNIT -> Lwt.return ()
      | Core.FAILURE (rc, msg) -> Lwt.fail (Common.XException(rc,msg))
      | Core.VALUE v -> failwith "Expected unit, not value"
	
  let _set driver k v = 
    let q = Core.SET(k,v) in
    __do_unit_update driver q
      
  let _admin_set driver k m_v = 
    let u = Core.ADMIN_SET(k, m_v) in
    __do_unit_update driver u
    
  let _sequence driver sequence = __do_unit_update driver sequence
    
  let _delete driver k =
    let q = Core.DELETE k in
    __do_unit_update driver q
      
  let _safe_get = S.get 
    
  let _get store k = 
    _safe_get store k >>= function
      | None -> Lwt.fail (Common.XException(Arakoon_exc.E_NOT_FOUND, k))
      | Some v -> Lwt.return v 

  let _prefix_keys store k max = S.prefix_keys store k max 

  let extract_master_info = function
    | None -> None
    | Some s -> 
      begin
        let m, off = Llio.string_option_from s 0 in m
      end
        
  let am_i_master store me = 
    S.get_meta store >>= fun meta ->
    match (extract_master_info meta) with
      | Some m when m = me -> Lwt.return true
      | _ -> Lwt.return false
  
  let _get_meta store = S.get_meta store 
    
  let _last_entries store i oc = S.last_entries store (Core.TICK i) oc
    
  let one_command me driver store ((ic,oc) as conn) =
    
    let only_if_master allow_dirty f =
      am_i_master store me >>= fun me_master ->
      Lwt.catch
        (fun () -> 
          if me_master || allow_dirty
          then f ()
          else Lwt.fail (Common.XException(Arakoon_exc.E_NOT_MASTER, me))
      ) 
      (Client_protocol.handle_exception oc)
    in
    let do_admin_set key rest =
      let ser = Pack.input_string rest in
      let do_inner () =
        _admin_set driver key (Some ser) >>= fun () ->
        Client_protocol.response_ok_unit oc
      in
      only_if_master false do_inner
    in
    let _do_range rest inner output = 
      let (allow_dirty, first, finc, last, linc, max) = get_range_params rest in
      let so2s = Log_extra.string_option_to_string in
      Lwtc.log "_do_range %s %b %s %b %s"
        (so2s first) finc (so2s last) linc 
        (Log_extra.int_option_to_string max) 
      >>= fun () ->
      only_if_master allow_dirty 
        (fun () -> 
          inner store first finc last linc max >>= fun l ->
          Llio.output_int oc 0 >>= fun () ->
          output oc (List.rev l) >>= fun () ->
          Lwt.return false
        )
    in 
    my_read_command conn >>= fun (comm, rest) ->
    let input_value (input:Pack.input) = 
      let vs = Pack.input_vint input in
      assert (vs < 8 * 1024 * 1024);
      Pack.input_raw input vs
    in
    match comm with
      | Common.WHO_MASTER ->
	Lwtc.log "who master" >>= fun () -> 
	_get_meta store >>= fun ms ->
        let mo = extract_master_info ms in
	Llio.output_int32 oc 0l >>= fun () ->
	Llio.output_string_option oc mo >>= fun () ->
	Lwt.return false
      | Common.SET -> 
	begin
	  let key = Pack.input_string rest in
	  let value = input_value rest in
	  Lwt.catch
	    (fun () -> 
	      _set driver key value >>= fun () ->
	      Client_protocol.response_ok_unit oc)
	    (Client_protocol.handle_exception oc)
	end
      | Common.GET ->
	begin
	  let allow_dirty =Pack.input_bool rest in
	  let key = Pack.input_string rest in
          Lwtc.log "GET %b %S" allow_dirty key >>= fun () ->
          let do_get () =
            _get store key >>= fun value ->
            Client_protocol.response_rc_string oc 0l value
          in
          only_if_master allow_dirty do_get
	end 
      | Common.DELETE ->
        let key = Pack.input_string rest in
        Lwtc.log "DELETE %S" key >>= fun () ->
        let do_delete () =
          _delete driver key >>= fun () ->
          Client_protocol.response_ok_unit oc
        in 
        only_if_master false do_delete
          
      | Common.LAST_ENTRIES ->
	begin
          let i = Pack.input_vint64 rest in
          Lwtc.log "LAST_ENTRIES %Li" i >>= fun () ->
	  Llio.output_int32 oc 0l >>= fun () ->
	  _last_entries store i oc >>= fun () ->
	  Sn.output_sn oc (Sn.of_int (-1)) >>= fun () ->
	  Lwtc.log "end of command" >>= fun () ->
	  Lwt.return false
	end
      | Common.SEQUENCE ->
	begin
	  Lwt.catch
	    (fun () ->
	      begin
                let data = Pack.input_string rest in
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
      | Common.MULTI_GET ->
	begin
	  let allow_dirty = Pack.input_bool rest in
          let keys = Pack.input_list Pack.input_string rest in
          let do_multi_get () = 
            Lwt_list.map_s (fun k -> _get store k ) keys >>= fun values ->
	    Llio.output_int oc 0>>= fun () ->
            Llio.output_list Llio.output_string oc values >>= fun () ->
	    Lwt.return false
	  in
          only_if_master allow_dirty do_multi_get
	end
      | Common.RANGE ->             _do_range rest S.range (Llio.output_list Llio.output_string)
      | Common.REV_RANGE_ENTRIES -> _do_range rest S.rev_range_entries Llio.output_kv_list
      | Common.RANGE_ENTRIES ->     _do_range rest S.range_entries Llio.output_kv_list 
      | Common.EXISTS ->
        let allow_dirty  = Pack.input_bool rest in
        let key = Pack.input_string rest in
        let do_exists () =
          _safe_get store key >>= fun m_val ->
          Llio.output_int oc 0 >>= fun () ->
          let r = 
            match m_val with
              | None -> false
              | Some _ -> true 
          in
          Llio.output_bool oc r
          >>= fun () ->
          Lwt.return false
        in
        only_if_master allow_dirty do_exists
      | Common.ASSERT ->
        let allow_dirty = Pack.input_bool rest in
        let key = Pack.input_string rest in
        let req_val = Pack.input_string_option rest in
        Lwtc.log "ASSERT: allow_dirty:%b key:%s req_val:%s" allow_dirty key 
          (Log_extra.string_option_to_string req_val) 
        >>= fun () ->
        let do_assert () =
          _safe_get store key >>= fun m_val ->
          if m_val <> req_val 
          then
            Lwt.fail (Common.XException(Arakoon_exc.E_ASSERTION_FAILED, key))
          else 
            Llio.output_int oc 0 >>= fun () ->
          Lwt.return false
        in
        only_if_master allow_dirty do_assert
        
      | Common.CONFIRM ->
        begin
          let key = Pack.input_string rest in
          let value = Pack.input_string rest in
          let do_confirm () =
            begin 
              _safe_get store key >>= fun v ->
              if v <> Some value 
              then
                _set driver key value 
              else 
                Lwt.return () 
            end
            >>= fun () -> 
            Client_protocol.response_ok_unit oc
          in 
          only_if_master false do_confirm
	      end
      | Common.TEST_AND_SET ->
        let key = Pack.input_string rest in 
        let m_old = Pack.input_string_option rest in
        let m_new = Pack.input_string_option rest in
        Lwtc.log "TEST_AND_SET key:%S m_old:%s m_new:%s" key 
          (Log_extra.string_option_to_string m_old)
          (Log_extra.string_option_to_string m_new)
        >>= fun () ->
        let do_test_and_set () = 
          _safe_get store key >>= fun m_val ->
          begin
            if m_val = m_old 
            then begin
              match m_new with
              | None -> Lwtc.log "Test_and_set: delete" >>= fun () -> _delete driver key
              | Some v -> Lwtc.log "Test_and_set: set" >>= fun () ->  _set driver key v
            end 
          else begin
            Lwtc.log "Test_and_set: nothing to be done"
            end
          end >>= fun () ->
          send_string_option oc m_val >>= fun () ->
          Lwt.return false
        in
        only_if_master false do_test_and_set 
      | Common.PREFIX_KEYS ->
        Lwtc.log "PREFIX_KEYS" >>= fun () ->
        let allow_dirty = Pack.input_bool rest in
        let key = Pack.input_string rest in
        let max = Pack.input_option Pack.input_vint rest in
        Lwtc.log "PREFIX_KEYS allow_dirty:%b key:%s max:%s" 
          allow_dirty key (Log_extra.int_option_to_string max)

        >>= fun () ->
        let do_prefix_keys () =
          _prefix_keys store key max >>= fun keys ->
          Lwtc.log "PREFIX_KEYS: result: [%s]" (String.concat ";" keys) >>= fun () ->
          Llio.output_int oc 0 >>= fun () ->
          Llio.output_list Llio.output_string oc (List.rev keys) >>= fun () ->
          Lwt.return false
        in
        only_if_master allow_dirty do_prefix_keys
      | Common.PING ->
        let client_id = Pack.input_string rest in
        let cluster_id = Pack.input_string rest in
        Llio.output_int oc 0 >>= fun () ->
        let msg = Printf.sprintf "Arakoon %S" Version.git_info in
        Llio.output_string oc msg >>= fun () ->
        Lwt.return false
      | Common.SET_ROUTING -> do_admin_set __routing_key rest
      | Common.SET_INTERVAL -> do_admin_set __interval_key rest
        
	 (*| _ -> Client_protocol.handle_exception oc (Failure "Command not implemented (yet)") *)
	      
  let protocol me driver store (ic,oc) =   
    let rec loop () = 
      begin
	one_command me driver store (ic,oc) >>= fun stop ->
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
