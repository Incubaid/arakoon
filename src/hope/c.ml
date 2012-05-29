open Lwt
open Modules
open Statistics
open Routing
open Interval

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
      if magic = _MAGIC && version = _VERSION 
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
    
  let __do_unit_update driver q =
    DRIVER.push_cli_req driver q >>= fun a ->
    match a with 
      | Core.UNIT -> Lwt.return ()
      | Core.FAILURE (rc, msg) -> Lwt.fail (Common.XException(rc,msg))
      | Core.VALUE_OPTION vo -> failwith "Expected unit, not value"
        
  let _set driver k v = 
    let q = Core.SET(k,v) in
    __do_unit_update driver q
      
  let _admin_set driver k m_v = 
    let u = Core.ADMIN_SET(k, m_v) in
    __do_unit_update driver u
  
  let _user_function driver name po = 
    let q = Core.USER_FUNCTION(name, po) in
    DRIVER.push_cli_req driver q >>= fun a ->
     match a with
       | Core.UNIT -> failwith "Expected value, not unit"
       | Core.FAILURE (rc, msg) -> Lwt.fail (Common.XException(rc,msg))
       | Core.VALUE_OPTION vo -> return vo

  let _admin_get store k = 
    S.admin_get store k >>= function
      | None -> Lwt.fail (Common.XException(Arakoon_exc.E_NOT_FOUND, k))
      | Some v -> Lwt.return v
    
  let _sequence driver sequence = __do_unit_update driver sequence
    
  let _delete driver k =
    let q = Core.DELETE k in
    __do_unit_update driver q
      
  let _safe_get = S.get 
    

  let _get_key_count store = S.get_key_count store


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

  let _close_write oc output =
    let buf = Pack.close_output output in
    Lwtc.log "BUFFER=%S" buf >>= fun () ->
    Lwt_io.write oc buf >>= fun () ->
    Lwt.return false    

  let _output_simple_error oc rc msg = 
    let size = String.length msg + 3 in
    let out = Pack.make_output size in
    Pack.vint_to out (Arakoon_exc.int_of_rc rc);
    Pack.string_to out msg;
    _close_write oc out

  let one_command me (stats:Statistics.t) driver store ((ic,oc) as conn) =
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
    let do_write_op f = 
      Lwt.catch
      ( fun () ->
        if S.is_read_only store 
        then Lwt.fail( Common.XException(Arakoon_exc.E_READ_ONLY, me ) )
        else only_if_master false f 
      ) (Client_protocol.handle_exception oc)
    in
    let do_admin_set key value =
      let do_inner () =
        _admin_set driver key (Some value) >>= fun () ->
        Client_protocol.response_ok_unit oc
      in
      do_write_op do_inner
    in
    let do_admin_get key =
      let do_inner () =
        _admin_get store key >>= fun res ->
        Client_protocol.response_rc_string oc 0l res
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
          output l
        )
    in 
    my_read_command conn >>= fun (comm, rest) ->
    let input_value (input:Pack.input) = 
      let vs = Pack.input_vint input in
      assert (vs < 8 * 1024 * 1024);
      Pack.input_raw input vs
    in
    let output_ok_string_option so = 
      let size = 64 (* TODO: better guess *) in
      let out = Pack.make_output size in
      Pack.vint_to out 0;
      Pack.string_option_to out so;
      _close_write oc out
    in
    let output_ok_unit () = 
      let size = 64 in
      let out = Pack.make_output size in
      Pack.vint_to out 0;
      _close_write oc out
    in
    let output_ok_bool b =
      let out = Pack.make_output 1 in
      Pack.bool_to out b;
      _close_write oc out
    in
    let output_ok_int i = 
      let size = 4 in
      let out = Pack.make_output size in
      Pack.vint_to out 0;
      Pack.vint_to out i;
      _close_write oc out
    in
    let output_ok_string s = 
      let size = String.length s + 2 in
      let out = Pack.make_output size in
      Pack.vint_to out 0;
      Pack.string_to out s;
      _close_write oc out
    in
    let output_ok_list e_to l =
      let size = 1024 in
      let out = Pack.make_output size in
      Pack.vint_to out 0;
      Pack.list_to out e_to l;
      _close_write oc out
    in
    let output_ok_string_list s = output_ok_list Pack.string_to s in
    let output_ok_kv_list s = 
      let e_to out (k,v) = 
        Pack.string_to out k;
        Pack.string_to out v
      in
      output_ok_list e_to s
    in
    let output_ok_statistics s = 
      let out = Pack.make_output 128 in
      Pack.vint_to out 0;
      Statistics.statistics_to out s;
      _close_write oc out
    in
    let unit_or_f a = 
      match a with 
        | Core.UNIT -> output_ok_unit ()
        | Core.FAILURE (rc, msg) -> _output_simple_error oc rc msg
        | Core.VALUE_OPTION vo -> failwith "Expected unit, not value"
    in
    match comm with
      | Common.WHO_MASTER ->
        Lwtc.log "who master" >>= fun () -> 
        _get_meta store >>= fun ms ->
        let mo = extract_master_info ms in
        output_ok_string_option mo
      | Common.SET -> 
        begin
          let key = Pack.input_string rest in
          let value = input_value rest in
          let do_set () = 
            let t0 = Unix.gettimeofday() in
            _set driver key value >>= fun () ->
            Statistics.new_set stats key value t0;
            output_ok_unit ()
          in 
          do_write_op do_set
        end
      | Common.GET ->
        begin
          let allow_dirty =Pack.input_bool rest in
          let key = Pack.input_string rest in
          Lwtc.log "GET %b %S" allow_dirty key >>= fun () ->
          let do_get () =
            let t0 = Unix.gettimeofday() in
            _safe_get store key >>= fun vo ->
            match vo with
              | None -> _output_simple_error oc Arakoon_exc.E_NOT_FOUND key
              | Some v ->  
                Statistics.new_get stats key v t0;
                output_ok_string v
          in
          only_if_master allow_dirty do_get
        end 
      | Common.DELETE ->
        let key = Pack.input_string rest in
        Lwtc.log "DELETE %S" key >>= fun () ->
        let do_delete () =
          let t0 = Unix.gettimeofday() in
          DRIVER.push_cli_req driver (Core.DELETE key) >>= fun a ->
          Statistics.new_delete stats t0;
          unit_or_f a
        in 
        do_write_op do_delete
          
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
        Lwtc.log "SEQUENCE" >>= fun () ->
        begin
          let do_sequence () =
            let t0 = Unix.gettimeofday() in
            let data = Pack.input_string rest in
            let probably_sequence,_ = Core.update_from data 0 in
            let sequence = match probably_sequence with
              | Core.SEQUENCE _ -> probably_sequence
              | _ -> raise (Failure "should be update")
            in
            DRIVER.push_cli_req driver sequence >>= fun a ->
            Statistics.new_sequence stats t0;
            unit_or_f a
          in do_write_op do_sequence 
        end
      | Common.MULTI_GET ->
        begin
          let allow_dirty = Pack.input_bool rest in
          let keys = Pack.input_list rest Pack.input_string in
          let do_multi_get () = 
            let t0 = Unix.gettimeofday() in
            let rec loop acc = function
              | [] -> 
                Statistics.new_multiget stats t0;
                output_ok_string_list (List.rev acc)
              | k :: ks ->
                _safe_get store k >>= fun vo ->
                match vo with
                  | None -> _output_simple_error oc Arakoon_exc.E_NOT_FOUND k
                  | Some v -> loop (v :: acc) ks
            in
            loop [] keys 
          in
          only_if_master allow_dirty do_multi_get
        end
        
      | Common.RANGE ->             _do_range rest S.range output_ok_string_list
      | Common.REV_RANGE_ENTRIES -> _do_range rest S.rev_range_entries output_ok_kv_list
      | Common.RANGE_ENTRIES ->     _do_range rest S.range_entries output_ok_kv_list
      | Common.EXISTS ->
        let allow_dirty  = Pack.input_bool rest in
        let key = Pack.input_string rest in
        let do_exists () =
          _safe_get store key >>= fun m_val ->
          output_ok_bool (m_val <> None)
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
            _output_simple_error oc Arakoon_exc.E_ASSERTION_FAILED key
          else 
            output_ok_unit ()
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
              then _set driver key value 
              else Lwt.return () 
            end
            >>= fun () -> 
            Client_protocol.response_ok_unit oc
          in 
          do_write_op do_confirm
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
          let t0 = Unix.gettimeofday() in
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
          Statistics.new_testandset stats t0;
          output_ok_string_option m_val 
        in
        do_write_op do_test_and_set 
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
          output_ok_string_list keys
        in
        only_if_master allow_dirty do_prefix_keys
      | Common.PING ->
        let client_id = Pack.input_string rest in
        let cluster_id = Pack.input_string rest in
        let msg = Printf.sprintf "Arakoon %S" Version.git_info in
        output_ok_string msg
      | Common.SET_ROUTING -> 
        let r = Routing.routing_from rest in
        let o = Pack.make_output 16 in
        Routing.routing_to o r;
        let v = Pack.close_output o in
        do_admin_set __routing_key v
      | Common.SET_INTERVAL -> 
        let i = Interval.interval_from rest in
        let o = Pack.make_output 16 in
        Interval.interval_to o i;
        let v = Pack.close_output o in
        do_admin_set __interval_key v
      | Common.GET_INTERVAL -> do_admin_get __interval_key 
      | Common.GET_ROUTING -> do_admin_get __routing_key 
      | Common.STATISTICS -> 
        Lwtc.log "STATISTICS" >>= fun () ->
        output_ok_statistics stats
      | Common.GET_DB ->
        begin
          Lwtc.log "GET_DB" >>= fun () ->
          Lwt.catch
            (fun () -> 
              Llio.output_int oc 0 >>= fun () ->
              S.raw_dump store oc >>= fun () ->
              Lwt.return true)
            (Client_protocol.handle_exception oc)
        end
      | Common.GET_KEY_COUNT ->
        begin
          Lwtc.log "GET_KEY_COUNT" >>= fun () ->
          _get_key_count store >>= fun kc ->
          output_ok_int kc
        end
      | Common.EXPECT_PROGRESS_POSSIBLE ->
        begin
          Lwtc.log "EXPECT_PROGRESS_POSSIBLE" >>= fun () ->
          _get_meta store >>= fun ms ->
          let mo = extract_master_info ms in
          let r = mo <> None in
          output_ok_bool r
        end
      | Common.USER_FUNCTION ->
        begin
          let name = Pack.input_string rest in
          let po   = Pack.input_string_option rest in
          Lwtc.log "USER_FUNCTION %S %S" name (Log_extra.string_option_to_string po) >>= fun () ->
          Lwt.catch 
            (fun () -> _user_function driver name po >>= fun ro ->
                       Llio.output_int oc 0 >>= fun () ->
                       Llio.output_string_option oc ro >>= fun () ->
                       Lwt.return false
            )
            (Client_protocol.handle_exception oc)
        end
            
  (*| _ -> Client_protocol.handle_exception oc (Failure "Command not implemented (yet)") *)
              
  let protocol me (stats:Statistics.t) driver store (ic,oc) =   
    let rec loop () = 
      begin
        one_command me stats driver store (ic,oc) >>= fun stop ->
        if stop
        then Lwtc.log "end of session: %s" me
        else 
          begin
            Lwt_io.flush oc >>= fun () ->
            loop ()
          end
      end
    in
    Lwtc.log "session started: %s" me >>= fun () ->
    prologue(ic,oc) >>= fun () ->
    Lwtc.log "prologue ok: %s" me >>= fun () ->
    loop ()
      
end
