open Lwt
open Modules
open Statistics
open Vcommon
open Common

module V1(S:Core.STORE) = struct
  module V = VCommon(S)

  let _read_command (ic,oc) =
    Llio.input_int32 ic >>= fun masked ->
    let magic = Int32.logand masked _MAGIC in
    begin
      if magic <> _MAGIC
      then
        begin
	      Llio.output_int32 oc 1l >>= fun () ->
	      Llio.lwt_failfmt "%lx has no magic" masked
        end
      else
        begin
	      let as_int32 = Int32.logand masked _MASK in
	      try
	        let c = lookup_code as_int32 in
            Lwt.return c
	      with Not_found ->
            Llio.output_int32 oc 5l >>= fun () ->
	        let msg = Printf.sprintf "%lx: command not found" as_int32 in
	        Llio.output_string oc msg >>= fun () ->
            Lwt.fail (Failure msg)
        end
    end

  let _response_ok_string oc string = 
    Llio.output_int32 oc 0l >>= fun () ->
    Llio.output_string oc string >>= fun () ->
    Lwt.return false

  let _response_ok_bool oc bool = 
    Llio.output_int32 oc 0l >>= fun () ->
    Llio.output_bool oc bool >>= fun () ->
    Lwt.return false

  let _response_ok_int oc i =
    Llio.output_int32 oc 0l >>= fun () ->
    Llio.output_int oc i >>= fun () ->
    Lwt.return false

  let _response_ok_string_option oc so =
    Llio.output_int32 oc 0l >>= fun () ->
    Llio.output_string_option oc so >>= fun () ->
    Lwt.return false

  let _response_ok_string_list oc ss =
    Llio.output_int32 oc 0l >>= fun () ->
    Llio.output_list Llio.output_string oc ss >>= fun () ->
    Lwt.return false

  let _response_ok_kv_list oc kvs = 
    Llio.output_int32 oc 0l >>= fun () ->
    let size = List.length kvs in
    Lwt_log.debug_f "size = %i" size >>= fun () ->
    Llio.output_list Llio.output_string_pair oc kvs >>= fun () ->
    Lwt.return false


  let _get_meta store = S.get_meta store 
    
  let __do_unit_update driver q =
    DRIVER.push_cli_req driver q >>= fun a ->
    match a with 
      | Core.VOID              -> Lwt.return ()
      | Core.FAILURE (rc, msg) -> Lwt.fail (Common.XException(rc,msg))
      | Core.VALUE v           -> failwith "Expected unit, not value"
        
  let _set driver k v = 
    let q = Core.SET(k,v) in
    __do_unit_update driver q    

  let _delete driver k = 
    let q = Core.DELETE k in
    __do_unit_update driver q

  let _handle_exception oc = V.handle_exception oc
    
  
      
  let _non_fatal oc rc msg = 
    Arakoon_exc.output_exception oc rc msg >>= fun () -> 
    Lwt.return false
      
  let _only_if_master (ic,oc) me store allow_dirty f =
    V.am_i_master store me >>= fun me_master ->
    if not (me_master || allow_dirty) 
    then _non_fatal oc Arakoon_exc.E_NOT_MASTER me
    else Lwt.catch f (_handle_exception oc)
      
      
      
  let _do_write_op (ic,oc) me store f = 
    Lwt.catch
      (fun () ->
        if S.is_read_only store 
        then Lwt.fail( Common.XException(Arakoon_exc.E_READ_ONLY, me ) )
        else _only_if_master (ic,oc) me store false f 
      ) 
      (V.handle_exception oc)
      
  let _unit_or_f oc = function
    | Core.VOID            -> V.response_ok_unit oc
    | Core.FAILURE(rc,msg) -> _non_fatal oc rc msg
    | Core.VALUE v         -> failwith "Expected unit, not value"
      
      
  let _do_ping (ic,oc) = 
    Llio.input_string_pair ic >>= fun (client_id,cluster_id) ->
    let msg = Printf.sprintf "Arakoon %S" Version.git_info in
    _response_ok_string oc msg
      
      
  let _do_set (ic,oc) me store stats driver = 
    Llio.input_string_pair ic >>= fun (key,value) ->
    let _inner () = 
      let t0 = Unix.gettimeofday() in
      _set driver key value >>= fun () ->
      Statistics.new_set stats key value t0;
      V.response_ok_unit oc
    in              
    _do_write_op (ic,oc) me store _inner
      
  let _do_delete (ic,oc) me store stats driver =
    Llio.input_string ic >>= fun key ->
    let _inner () = 
      let t0 = Unix.gettimeofday() in
      DRIVER.push_cli_req driver (Core.DELETE key) >>= fun a ->
      Statistics.new_delete stats t0;
      _unit_or_f oc a
    in
    _do_write_op (ic,oc) me store _inner 

  let _do_delete_prefix (ic,oc) me store stats driver = 
    Llio.input_string ic >>= fun prefix ->
    let _inner () =
      let t0 = Unix.gettimeofday () in
      DRIVER.push_cli_req driver (Core.DELETE_PREFIX prefix) >>= fun a ->
      (* Statistics.new_delete_prefix stats t0; *)
      match a with
        | Core.VALUE v -> 
          let c,_ = Llio.int_from v 0 in
          _response_ok_int oc c
        | _            -> failwith "expected int, not this"
    in
    _do_write_op (ic,oc) me store _inner
    
      
  let _do_get (ic,oc) me store stats = 
    Llio.input_bool   ic >>= fun allow_dirty ->
    Llio.input_string ic >>= fun key ->
    let _inner () = 
      let t0 = Unix.gettimeofday() in
      S.get store key >>= fun vo ->
      match vo with
        | None   -> _non_fatal oc Arakoon_exc.E_NOT_FOUND key
        | Some v -> Statistics.new_get stats key v t0;
          _response_ok_string oc v
    in
    _only_if_master (ic,oc) me store allow_dirty _inner

  let _do_exists (ic,oc) me store stats =
    Llio.input_bool   ic >>= fun allow_dirty ->
    Llio.input_string ic >>= fun key ->
    let _inner () = 
      (* let t0 = Unix.gettimeofday () in *)
      S.get store key >>= fun vo ->
      _response_ok_bool oc (vo <> None)
    in
    _only_if_master (ic,oc) me store allow_dirty _inner
      
  let _do_who_master (_,oc) store =
    _get_meta store >>= fun ms ->
    let mo = Core.extract_master_info ms in
    _response_ok_string_option oc mo
    
  let _do_range_generic (ic,oc) me store stats query response = 
    Llio.input_bool          ic >>= fun allow_dirty ->
    Llio.input_string_option ic >>= fun first ->
    Llio.input_bool          ic >>= fun finc ->
    Llio.input_string_option ic >>= fun last ->
    Llio.input_bool          ic >>= fun linc ->
    Llio.input_int           ic >>= fun max  ->
    let so2s = Log_extra.string_option_to_string in
    Lwtc.log "_do_range_generic %s %b %s %b %i" 
      (so2s first) finc (so2s last) linc max >>= fun () ->
    let _inner () = 
      query store first finc last linc (Some max) >>= fun keys ->
      response oc keys
    in
    _only_if_master (ic,oc) me store allow_dirty _inner    
  
  let _do_range (ic,oc) me store stats = 
    _do_range_generic (ic,oc) me store stats 
      S.range _response_ok_string_list
  let _do_range_entries (ic,oc) me store stats = 
    _do_range_generic (ic,oc) me store stats 
      S.range_entries _response_ok_kv_list
    
    
    
  let _do_prefix_keys (ic,oc) me store stats = 
    Lwtc.log "_do_prefix_keys" >>= fun () ->
    Llio.input_bool    ic >>= fun allow_dirty ->
    Llio.input_string  ic >>= fun key ->
    Llio.input_int     ic >>= fun max ->
    Lwtc.log "_do_prefix_keys %b %s %i" allow_dirty key max >>= fun () ->
    let _inner () = 
      let max' = if max = -1 then None else Some max in
      S.prefix_keys store key max' >>= fun keys ->
      Lwtc.log "keys: %S" (Log_extra.string_of_list (fun s -> s) keys) >>= fun () ->
      let rev_keys = List.rev keys in
      _response_ok_string_list oc rev_keys
    in
    _only_if_master (ic,oc) me store allow_dirty _inner
    
  let _do_test_and_set (ic,oc) me store stats driver = 
    Llio.input_string        ic >>= fun key ->
    Llio.input_string_option ic >>= fun expected ->
    Llio.input_string_option ic >>= fun wanted ->
    let _inner () = 
      let t0 = Unix.gettimeofday () in
      S.get store key >>= fun m_val ->
      begin
        if m_val = expected
        then 
          begin
            match wanted with
              | None   -> _delete driver key 
              | Some v -> _set driver key v
          end
        else
          Lwt.return ()
      end >>= fun () ->
      Statistics.new_testandset stats t0;
      _response_ok_string_option oc m_val
    in
    _do_write_op (ic,oc) me store _inner

  let _do_assert (ic,oc) me store stats = 
    Llio.input_bool          ic >>= fun allow_dirty ->
    Llio.input_string        ic >>= fun key ->
    Llio.input_string_option ic >>= fun vo  ->
    let _inner () = 
      S.get store key >>= fun m_val ->
      begin
        if m_val <> vo
        then _non_fatal oc Arakoon_exc.E_ASSERTION_FAILED key
        else V.response_ok_unit oc
      end
    in
    _only_if_master (ic,oc) me store allow_dirty _inner

  let _do_assert_exists (ic,oc) me store stats = 
    Llio.input_bool          ic >>= fun allow_dirty ->
    Llio.input_string        ic >>= fun key ->
    let _inner () = 
      S.get store key >>= fun m_val ->
      begin
        if m_val = None
        then _non_fatal oc Arakoon_exc.E_ASSERTION_FAILED key
        else V.response_ok_unit oc
      end
    in
    _only_if_master (ic,oc) me store allow_dirty _inner


  let _do_sequence (ic,oc) me store stats driver = 
    Llio.input_string ic >>= fun data ->
    let _inner () = 
      let t0 = Unix.gettimeofday () in
      let probably_sequence, _ = Core.update_from data 0 in
      let sequence = match probably_sequence with
        | Core.SEQUENCE _ -> probably_sequence
        | _ -> raise (Failure "should be sequence")
      in
      DRIVER.push_cli_req driver sequence >>= fun a ->
      Statistics.new_sequence stats t0;
      _unit_or_f oc a
    in
    _do_write_op (ic,oc) me store _inner

  let _do_multi_get (ic,oc) me store stats = 
    Llio.input_bool         ic >>= fun allow_dirty ->
    Llio.input_string_list  ic >>= fun keys ->
    let _inner () = 
      let t0 = Unix.gettimeofday () in
      let rec loop acc = function
        | [] ->
          Statistics.new_multiget stats t0;
          _response_ok_string_list oc (List.rev acc)
        | k :: ks ->
          S.get store k >>= fun vo ->
          match vo with
            | None ->  _non_fatal oc Arakoon_exc.E_NOT_FOUND k
            | Some v -> loop (v :: acc) ks
      in
      loop [] keys
    in
    _only_if_master (ic,oc) me store allow_dirty _inner

  let _do_expect_pp (ic,oc) store = 
    _get_meta store >>= fun ms ->
    let mo = Core.extract_master_info ms in    
    let r = mo <> None in
    _response_ok_bool oc r

  let _do_statistics (ic,oc) stats = _non_fatal oc Arakoon_exc.E_UNKNOWN_FAILURE "not supported"

  let _do_version (ic,oc) =
    Llio.output_int oc 0 >>= fun () ->
    Llio.output_int oc Version.major >>= fun () ->
    Llio.output_int oc Version.minor >>= fun () ->
    Llio.output_int oc Version.patch >>= fun () ->
    Lwtc.log "%i.%i.%i" Version.major Version.minor Version.patch >>= fun () ->
    let rest = Printf.sprintf "revision: %S\ncompiled: %S\nmachine: %S\n"
      Version.git_info
      Version.compile_time
      Version.machine
    in
    Llio.output_string oc rest >>= fun () ->
    Lwt.return false

  type connection = Lwt_io.input_channel * Lwt_io.output_channel

  let one_command (me:string) (stats:Statistics.t) driver store (conn:connection) = 
    _read_command conn >>= fun c -> 
    let fail () = failwith (Printf.sprintf "%li not backward compatible yet" (List.assoc c Common.code2int)) in
    match c with
      | Common.PING                      -> _do_ping          conn
      | Common.WHO_MASTER                -> _do_who_master    conn    store
      | Common.EXISTS                    -> _do_exists        conn me store stats
      | Common.GET                       -> _do_get           conn me store stats
      | Common.SET                       -> _do_set           conn me store stats driver
      | Common.DELETE                    -> _do_delete        conn me store stats driver 
      | Common.RANGE                     -> _do_range         conn me store stats
      | Common.PREFIX_KEYS               -> _do_prefix_keys   conn me store stats
      | Common.TEST_AND_SET              -> _do_test_and_set  conn me store stats driver
      | Common.LAST_ENTRIES              -> fail ()
      | Common.RANGE_ENTRIES             -> _do_range_entries conn me store stats
      | Common.SEQUENCE                  -> _do_sequence      conn me store stats driver
      | Common.MULTI_GET                 -> _do_multi_get     conn me store stats
      | Common.EXPECT_PROGRESS_POSSIBLE  -> _do_expect_pp     conn    store
      | Common.STATISTICS                -> _do_statistics    conn          stats
      | Common.USER_FUNCTION             -> fail ()
      | Common.ASSERT                    -> _do_assert        conn me store stats
      | Common.ASSERT_EXISTS             -> _do_assert_exists conn me store stats
      | Common.SET_INTERVAL              -> fail ()
      | Common.DELETE_PREFIX             -> _do_delete_prefix conn me store stats driver
      | Common.VERSION                   -> _do_version       conn
        
      | c -> fail ()
end
