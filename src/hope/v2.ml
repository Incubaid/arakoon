open Statistics
open Lwt
open Baardskeerder (* for Pack *)
open Common
open Routing
open Interval
open Client_cfg
open Mp

module V2(S:Core.STORE) (A:MP_ACTION_DISPATCHER) = struct
  module V = Vcommon.VCommon(S)(A)
  module D = Mp_driver.MPDriver(A)

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
          Lwt.return (c, Pack.make_input rest 0)
        with Not_found ->
          Llio.output_int32 oc 5l >>= fun () ->
          let msg = Printf.sprintf "%08lx: command not found" as_int32 in
          Llio.output_string oc msg >>= fun () ->
          Lwt.fail (Failure msg)
      end

  let get_range_params input =
    let allow_dirty = Pack.input_bool input in
    let first = Pack.input_string_option input in 
    let finc = Pack.input_bool input in
    let last = Pack.input_string_option input in
    let linc = Pack.input_bool input in
    let max = Pack.input_option Pack.input_vint input in
    (allow_dirty, first, finc, last, linc, max)


      
  let _close_write oc output =
    let buf = Pack.close_output output in
    Lwtc.log "BUFFER(%i)=%S" (String.length buf) buf >>= fun () ->
    Lwt_io.write oc buf >>= fun () ->
    Lwt.return false    

  let _output_simple_error oc rc msg = 
    let size = String.length msg + 3 in
    let out = Pack.make_output size in
    Pack.vint_to out (Arakoon_exc.int_of_rc rc);
    Pack.string_to out msg;
    _close_write oc out (* ?? *)

  let _output_ok_unit oc = 
    let size = 64 in
    let out = Pack.make_output size in
    Pack.vint_to out 0;
    _close_write oc out


  let _output_ok_bool oc b =
    let out = Pack.make_output 2 in
    Pack.vint_to out 0;
    Pack.bool_to out b;
    _close_write oc out

  
let _set driver k v = 
    let q = Core.SET(k,v) in
    V.do_unit_update driver q

  let _delete driver k =
    let q = Core.DELETE k in
    V.do_unit_update driver q

  let _prefix_keys store k max = S.prefix_keys store k max 

       
  let _sequence driver sequence = V.do_unit_update driver sequence
    
    
  let _last_entries store i oc = S.last_entries store (Core.ITick.from_int64 i) oc



  let _user_function driver name po oc = 
    let q = Core.USER_FUNCTION(name, po) in
    D.push_cli_req driver q >>= fun a ->
    let out = Pack.make_output 64 in
    begin
      match a with
        | Core.VOID -> 
          Pack.vint_to out 0; 
          Pack.string_option_to out None (* TODO: shouldn't we fail ? *)
        | Core.FAILURE (rc, msg) -> 
          Pack.vint_to out (Arakoon_exc.int_of_rc rc);
          Pack.string_to out msg
        | Core.VALUE v -> 
          Pack.vint_to out 0;
          Pack.string_option_to out (Some v)
    end;
    _close_write oc out

  let _test_and_set driver (k:string) (e:v option) (w:v option) oc = 
    let q = Core.TEST_AND_SET(k,e,w) in
    D.push_cli_req driver q >>= fun a ->
    Lwtc.log "_test_and_set result=%s" (Core.result2s a) >>= fun () ->
    let out = Pack.make_output 64 in
    begin
      match a with
        | Core.VOID -> 
            Pack.vint_to out 0;
            Pack.string_option_to out None
        | Core.VALUE v -> 
            Pack.vint_to out 0;
            Pack.string_option_to out (Some v)
        | Core.FAILURE (rc, msg) ->
            Pack.vint_to out  (Arakoon_exc.int_of_rc rc);
            Pack.string_to out msg
    end;
    _close_write oc out

  let _unit_or_f oc = function
    | Core.VOID              -> _output_ok_unit oc
    | Core.FAILURE (rc, msg) -> _output_simple_error oc rc msg
    | Core.VALUE _           -> failwith "Expected unit, not value"

  let _only_if_master rest oc me store allow_dirty f =
    V.am_i_master store me >>= fun me_master ->
    Lwt.catch
      (fun () -> 
        if me_master || allow_dirty
        then f ()
        else Lwt.fail (Common.XException(Arakoon_exc.E_NOT_MASTER, me))
      ) 
      (V.handle_exception oc)

  let _do_write_op rest oc me store f = 
    Lwt.catch
      ( fun () ->
        if S.is_read_only store 
        then Lwt.fail( Common.XException(Arakoon_exc.E_READ_ONLY, me ) )
        else _only_if_master rest oc me store false f 
      ) (V.handle_exception oc)


  let _do_user_function rest oc me store stats driver = 
    let name = Pack.input_string rest in
    let po   = Pack.input_string_option rest in
    Lwtc.log "USER_FUNCTION %S %S" name (Log_extra.string_option_to_string po) >>= fun () ->
    _user_function driver name po oc 

  let _do_test_and_set rest oc me store stats driver = 
    begin
      let k = Pack.input_string rest in 
      let e = Pack.input_string_option rest in
      let w = Pack.input_string_option rest in
      Lwtc.log "TEST_AND_SET key:%S e:%S w:%S" k
        (Log_extra.string_option_to_string e)
        (Log_extra.string_option_to_string w)
      >>= fun () ->
      _test_and_set driver k e w oc
          (*
          let inner () = 
            let t0 = Unix.gettimeofday() in
            S.get store key >>= fun m_val ->
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
          _do_write_op rest oc me store inner
          *)
    end


  let _do_delete_prefix rest oc  me store stats driver = 
    let key = Pack.input_string rest in
    Lwtc.log "DELETE_PREFIX %S" key >>= fun () ->
    let _inner () = 
      let t0 = Unix.gettimeofday () in
      D.push_cli_req driver (Core.DELETE_PREFIX key) >>= fun a ->
      let out = Pack.make_output 64 in
      begin
        match a with
        | Core.FAILURE (rc, msg) -> 
          Pack.vint_to out (Arakoon_exc.int_of_rc rc);
          Pack.string_to out msg
        | Core.VALUE v -> let i,_ = Llio.int_from v 0 in
                          Pack.vint_to out 0;
                          Pack.vint_to out i
        | Core.VOID  -> failwith "excpted int, not this"
      end;
      _close_write oc out
    in
    _do_write_op rest oc me store _inner 

  let _do_version oc = 
    Lwtc.log "VERSION" >>= fun () ->
    let out = Pack.make_output 128 in
    Pack.vint_to out 0;
    Pack.vint_to out Version.major;
    Pack.vint_to out Version.minor;
    Pack.vint_to out Version.patch;
    let rest = Printf.sprintf "revision: %S\ncompiled: %S\nmachine: %S\n"
      Version.git_info
      Version.compile_time
      Version.machine
    in
    Pack.string_to out rest;
    let s = Pack.close_output out in
    Lwt_io.write oc s >>= fun () -> Lwt.return false

  let one_command me (stats:Statistics.t) driver store ((ic,oc) as conn) =
    let do_admin_set rest key value =
      let do_inner () =
        V.admin_set driver key (Some value) >>= fun () ->
        V.response_ok_unit oc
      in
      _do_write_op rest oc me store do_inner
    in
    let do_admin_get rest key =
      let do_inner () =
        V.admin_get store key >>= fun res ->
        Lwt_log.debug_f "do_admin_get %s ==> %S" key res >>= fun () ->
        V.response_rc_string oc 0l res
      in
      _only_if_master rest oc me store false do_inner
    in
    let _do_range rest inner output = 
      let (allow_dirty, first, finc, last, linc, max) = get_range_params rest in
      let so2s = Log_extra.string_option_to_string in
      Lwtc.log "_do_range %s %b %s %b %s"
        (so2s first) finc (so2s last) linc 
        (Log_extra.int_option_to_string max) 
      >>= fun () ->
      _only_if_master rest oc me store allow_dirty 
        (fun () -> 
          inner store first finc last linc max >>= fun l ->
          Lwtc.log "length = %i" (List.length l) >>= fun () ->
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
    match comm with
      | Common.PING ->
        let client_id = Pack.input_string rest in
        let cluster_id = Pack.input_string rest in
        let msg = Printf.sprintf "Arakoon %S" Version.git_info in
        output_ok_string msg
      | Common.WHO_MASTER ->
        begin
          Lwtc.log "who master" >>= fun () -> 
          S.get_meta store >>= fun ms ->
          let mo = Core.extract_master_info ms in
          Lwtc.log "mo = %s" (Log_extra.string_option_to_string mo) >>= fun () ->
          output_ok_string_option mo
        end
      | Common.EXISTS ->
        begin
          let allow_dirty  = Pack.input_bool rest in
          let key = Pack.input_string rest in
          let do_exists () =
            S.get store key >>= fun m_val ->
            _output_ok_bool oc (m_val <> None)
          in
          _only_if_master rest oc me store allow_dirty do_exists
        end
      | Common.GET ->
        begin
          let allow_dirty =Pack.input_bool rest in
          let key = Pack.input_string rest in
          Lwtc.log "GET %b %S" allow_dirty key >>= fun () ->
          let do_get () =
            let t0 = Unix.gettimeofday() in
            S.get store key >>= fun vo ->
            match vo with
              | None -> _output_simple_error oc Arakoon_exc.E_NOT_FOUND key
              | Some v ->  
                Statistics.new_get stats key v t0;
                output_ok_string v
          in
          _only_if_master rest oc me store allow_dirty do_get
        end 
      | Common.SET -> 
        begin
          let key = Pack.input_string rest in
          let value = input_value rest in
          let do_set () = 
            let t0 = Unix.gettimeofday() in
            _set driver key value >>= fun () ->
            Statistics.new_set stats key value t0;
            _output_ok_unit oc
          in 
          _do_write_op rest oc me store do_set
        end
      | Common.DELETE ->
        begin
          let key = Pack.input_string rest in
          Lwtc.log "DELETE %S" key >>= fun () ->
          let do_delete () =
            let t0 = Unix.gettimeofday() in
            D.push_cli_req driver (Core.DELETE key) >>= fun a ->
            Statistics.new_delete stats t0;
            _unit_or_f oc a
          in 
          _do_write_op rest oc me store do_delete
        end
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
          Lwtc.log "SEQUENCE" >>= fun () ->
          let inner () =
            let t0 = Unix.gettimeofday() in
            let data = Pack.input_string rest in
            let probably_sequence,_ = Core.update_from data 0 in
            let sequence = match probably_sequence with
              | Core.SEQUENCE _ -> probably_sequence
              | _ -> raise (Failure "should be sequence")
            in
            D.push_cli_req driver sequence >>= fun a ->
            Statistics.new_sequence stats t0;
            _unit_or_f oc a
          in _do_write_op rest oc me store inner
        end
      | Common.MULTI_GET ->
        begin
          let allow_dirty = Pack.input_bool rest in
          let keys = Pack.input_list rest Pack.input_string in
          let inner () = 
            let t0 = Unix.gettimeofday() in
            let rec loop acc = function
              | [] -> 
                Statistics.new_multiget stats t0;
                output_ok_string_list (List.rev acc)
              | k :: ks ->
                S.get store k >>= fun vo ->
                match vo with
                  | None -> _output_simple_error oc Arakoon_exc.E_NOT_FOUND k
                  | Some v -> loop (v :: acc) ks
            in
            loop [] keys 
          in
          _only_if_master rest oc me store allow_dirty inner
        end
          
      | Common.RANGE ->             _do_range rest S.range output_ok_string_list
      | Common.REV_RANGE_ENTRIES -> _do_range rest S.rev_range_entries output_ok_kv_list
      | Common.RANGE_ENTRIES -> 
        begin
          Lwtc.log "RANGE_ENTRIES" >>= fun () ->
          _do_range rest S.range_entries output_ok_kv_list
        end
          
      | Common.ASSERT ->
        begin
          let allow_dirty = Pack.input_bool rest in
          let key = Pack.input_string rest in
          let req_val = Pack.input_string_option rest in
          Lwtc.log "ASSERT: allow_dirty:%b key:%s req_val:%s" allow_dirty key 
            (Log_extra.string_option_to_string req_val) 
          >>= fun () ->
          let inner () =
            S.get store key >>= fun m_val ->
            if m_val <> req_val 
            then
              _output_simple_error oc Arakoon_exc.E_ASSERTION_FAILED key
            else 
              _output_ok_unit oc
          in
          _only_if_master rest oc me store allow_dirty inner
        end
      | Common.ASSERT_EXISTS ->
        begin
          let allow_dirty = Pack.input_bool rest in
          let key = Pack.input_string rest in
          Lwtc.log "ASSERT_EXISTS: allow_dirty:%b key:%s" allow_dirty key 
          >>= fun () ->
          let inner () =
            S.get store key >>= fun m_val ->
            if m_val = None 
            then
              _output_simple_error oc Arakoon_exc.E_ASSERTION_FAILED key
            else 
              _output_ok_unit oc
          in
          _only_if_master rest oc me store allow_dirty inner
        end
      | Common.CONFIRM ->
        begin
          let key = Pack.input_string rest in
          let value = Pack.input_string rest in
          let inner () =
            begin 
              S.get store key >>= fun v ->
              if v <> Some value 
              then _set driver key value 
              else Lwt.return () 
            end
            >>= fun () -> 
            V.response_ok_unit oc
          in 
          _do_write_op rest oc me store inner
        end
      | Common.TEST_AND_SET -> _do_test_and_set rest oc me store stats driver
      | Common.PREFIX_KEYS ->
        begin
          Lwtc.log "PREFIX_KEYS" >>= fun () ->
          let allow_dirty = Pack.input_bool rest in
          let key = Pack.input_string rest in
          let max = Pack.input_option Pack.input_vint rest in
          Lwtc.log "PREFIX_KEYS allow_dirty:%b key:%s max:%s" 
            allow_dirty key (Log_extra.int_option_to_string max)
            
          >>= fun () ->
          let inner () =
            _prefix_keys store key max >>= fun keys ->
            Lwtc.log "PREFIX_KEYS: result: [%s]" (String.concat ";" keys) >>= fun () ->
            output_ok_string_list keys
          in
          _only_if_master rest oc me store allow_dirty inner
        end
          
      | Common.SET_ROUTING -> 
        begin
          let r = Routing.routing_from rest in
          let o = Pack.make_output 16 in
          Routing.routing_to o r;
          let v = Pack.close_output o in
          Lwt_log.debug_f "Setting routing key to %S" v >>= fun () -> 
          do_admin_set rest Core.__routing_key v
        end
      | Common.SET_INTERVAL -> 
        let i = Interval.interval_from rest in
        let o = Pack.make_output 16 in
        Interval.interval_to o i;
        let v = Pack.close_output o in
        do_admin_set rest Core.__interval_key v
      | Common.GET_INTERVAL -> 
        begin
          Lwt_log.debug "GET_INTERVAL" >>= fun () ->
          do_admin_get rest Core.__interval_key 
        end
      | Common.GET_ROUTING -> 
        begin
          Lwt_log.debug "GET_ROUTING" >>= fun () ->
          do_admin_get rest Core.__routing_key 
        end
      | Common.STATISTICS -> 
        begin
          Lwtc.log "STATISTICS" >>= fun () ->
          output_ok_statistics stats
        end
      | Common.GET_DB ->
        begin
          Lwtc.log "GET_DB" >>= fun () ->
          Lwt.catch
            (fun () -> 
              Llio.output_int oc 0 >>= fun () ->
              S.raw_dump store oc >>= fun () ->
              Lwt.return true)
            (V.handle_exception oc)
        end
      | Common.GET_KEY_COUNT ->
        begin
          Lwtc.log "GET_KEY_COUNT" >>= fun () ->
          S.get_key_count store >>= fun kc ->
          output_ok_int kc
        end
      | Common.EXPECT_PROGRESS_POSSIBLE ->
        begin
          Lwtc.log "EXPECT_PROGRESS_POSSIBLE" >>= fun () ->
          S.get_meta store >>= fun ms ->
          let mo = Core.extract_master_info ms in
          let r = mo <> None in
          _output_ok_bool oc r
        end
      | Common.SET_NURSERY_CFG ->
        begin
          Lwtc.log "SET_NURSERY_CFG" >>= fun () ->
          let cluster_id = Pack.input_string rest in
          let cfg = ClientCfg.cfg_from rest in
          let key = Core.__nursery_cluster_prefix ^ cluster_id in
          let out = Pack.make_output 16 in
          ClientCfg.cfg_to out cfg;
          let value = Pack.close_output out in 
          do_admin_set rest key value
        end
      | Common.GET_NURSERY_CFG ->
        begin
          Lwtc.log "GET_NURSERY_CFG" >>= fun () ->
          V.admin_get store Core.__routing_key >>= fun v ->
          let input = Pack.make_input v 0 in
          let rsize = Pack.input_size input in
          Lwt_log.debug "Decoding routing info" >>= fun () ->
          let r = Routing.routing_from input in
          let out = Pack.make_output 32 in
          Pack.vint_to out 0;
          Lwt_log.debug "Repacking routing info" >>= fun () ->
          Routing.routing_to out r;
          Lwt_log.debug "Fetching nursery clusters" >>= fun () ->
          S.admin_prefix_keys store Core.__nursery_cluster_prefix >>= fun clu_keys ->
          let clusters = Hashtbl.create 2 in
          let key_start = String.length Core.__nursery_cluster_prefix in
          Lwt_list.iter_s 
            (fun k -> 
              Lwt_log.debug_f "Fetch nursery cluster: %s" k >>= fun () ->
              S.admin_get store k >>= function
                | None -> failwith "nursery cluster disappeared??"
                | Some v ->
                  begin
                    let tail_size = (String.length k) - key_start in
                    Lwt_log.debug_f "Sub %s %d %d" k key_start tail_size >>= fun () ->
                    let clu_id = String.sub k key_start tail_size in
                    let input = Pack.make_input v 0 in
                    let input_size = Pack.input_size input in
                    let cfg = ClientCfg.cfg_from input in
                    Hashtbl.replace clusters clu_id cfg;
                    Lwt.return ()
                  end
            )
            clu_keys
          >>= fun () ->
          let pack_one out k e =
            Pack.string_to out k; 
            ClientCfg.cfg_to out e
          in
          Pack.hashtbl_to out pack_one clusters;
          let s = Pack.close_output out in
          Lwt_io.write oc s >>= fun () -> Lwt.return false  
        end
      | Common.USER_FUNCTION -> _do_user_function rest oc me store stats driver
      | Common.DELETE_PREFIX -> _do_delete_prefix rest oc me store stats driver
      | Common.VERSION       -> _do_version            oc

      | Common.GET_FRINGE ->
          begin
            let boundary = Pack.input_string_option rest in
            let direction_i = Pack.input_vint rest in
            let direction  = match direction_i with
              | 0 -> Routing.UPPER_BOUND
              | 1 -> Routing.LOWER_BOUND
              
            in
            Lwtc.log "GET_FRINGE: %s %i"  
              (Log_extra.string_option_to_string boundary) 
              direction_i >>= fun () ->
            S.get_fringe store boundary direction >>= fun kvs ->
            let out = Pack.make_output 4096 in
            Pack.vint_to out 0; 
            Pack.list_to out 
              (fun out ((k:string),(v:string)) -> 
                Pack.string_to out k;
                Pack.string_to out v;
              ) kvs;
            _close_write oc out
          end
    (*| _ -> Client_protocol.handle_exception oc (Failure "Command not implemented (yet)") *)
  end
