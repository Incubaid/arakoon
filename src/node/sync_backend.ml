(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

module Sync_backend = functor(S : Store.STORE) ->
struct

  open Std
  open Backend
  open Statistics
  open Client_cfg
  open Node_cfg
  open Lwt
  open Log_extra
  open Update
  open Interval
  open Common
  open Master_type
  open Arakoon_client

  let section = Logger.Section.main

  let _s_ = string_option2s

  let ncfg_prefix_b4 = "nursery.cfg."
  let ncfg_prefix_2far = "nursery.cfg/"
  let ncfg_prefix_b4_o = Some ncfg_prefix_b4
  let ncfg_prefix_2far_o = Some ncfg_prefix_2far

  exception Forced_stop

  let no_stats _ = ()

  let make_went_well (stats_cb:Store.update_result -> unit) awake sleeper =
    fun b ->
      begin
        Lwt.catch
          ( fun () ->Lwt.return (Lwt.wakeup awake b))
          ( fun e ->
             match e with
               | Invalid_argument _ ->
                 let t = state sleeper in
                 begin
                   match t with
                     | Fail ex ->
                       begin
                         Logger.error_  "Lwt.wakeup error: Sleeper already failed before. Re-raising"
                         >>= fun () ->
                         Lwt.fail ex
                       end
                     | Return _ ->
                       Logger.error_ "Lwt.wakeup error: Sleeper already returned"
                     | Sleep ->
                       Lwt.fail (Failure "Lwt.wakeup error: Sleeper is still sleeping however")
                 end
               | _ -> Lwt.fail e
          ) >>= fun () ->
        stats_cb b;
        Lwt.return ()
      end

  let _mute_so _ = ()

  let _update_rendezvous self ~so_post update update_stats push =
    self # _write_allowed ();
    let sleep, awake = Lwt.wait () in
    let went_well = make_went_well update_stats awake sleep in
    push (update, went_well) >>= fun () ->
    let t0 = Unix.gettimeofday() in
    sleep >>= fun r ->
    let t1 = Unix.gettimeofday() in
    let t = (t1 -. t0) in
    let lvl =
      if t > 1.0
      then Logger.Info
      else Logger.Debug
    in
    Logger.log_ section lvl (fun () -> Printf.sprintf "rendezvous (%s) took %f" (Update.update2s update) t) >>= fun () ->
    match r with
      | Store.Update_fail (rc,str) -> Lwt.fail (XException(rc,str))
      | Store.Ok so -> Lwt.return (so_post so)


  class sync_backend = fun cfg
    (push_update:Update.t * (Store.update_result -> unit Lwt.t) -> unit Lwt.t)
    (push_node_msg:Multi_paxos.paxos_event -> unit Lwt.t)
    (store: 'a)
    (store_methods: (string -> string ->
                     overwrite:bool -> throttling:float ->
                     unit Lwt.t) * string * float)
    (tlog_collection:Tlogcollection.tlog_collection)
    (lease_expiration:int)
    ~quorum_function n_nodes
    ~expect_reachable
    ~test
    ~(read_only:bool)
    ~max_value_size
    ~collapse_slowdown
    ~act_not_preferred ->
    let my_name =  Node_cfg.node_name cfg in
    let locked_tlogs = Hashtbl.create 8 in
    let blockers_cond = Lwt_condition.create() in
    let collapsing_lock = Lwt_mutex.create() in
    let assert_value_size value =
      let length = String.length value in
      if length >= max_value_size then
        raise (Arakoon_exc.Exception (Arakoon_exc.E_UNKNOWN_FAILURE,
                                      "value too large"))
    in
    let is_witness = cfg.Node_cfg.is_witness in
    let (_, _, copy_head_throttling) = store_methods in
    object(self: #backend)
      val witnessed = Hashtbl.create 10
      val _stats = Statistics.create ()
      val mutable client_cfgs = None



      method exists ~consistency key =
        self # _read_allowed consistency;
        S.exists store key

      method get ~consistency key =
        let start = Unix.gettimeofday () in
        self # _read_allowed consistency;
        self # _check_interval [key];
        let v = S.get store key in
        Statistics.new_get _stats key v start;
        v

      method get_interval () =
        self # _read_allowed Consistent;
        S.get_interval store


      method private block_collapser (i: Sn.t) =
        let tlog_file_n = tlog_collection # get_tlog_from_i i  in
        Hashtbl.add locked_tlogs tlog_file_n "locked"

      method private unblock_collapser i =
        let tlog_file_n = tlog_collection # get_tlog_from_i i in
        Hashtbl.remove locked_tlogs tlog_file_n;
        Lwt_condition.signal blockers_cond ()

      method private wait_for_tlog_release tlog_file_n =
        let blocking_requests = [] in
        let maybe_add_blocker tlog_num _ blockers =
          if tlog_file_n >= tlog_num
          then
            tlog_num :: blockers
          else
            blockers
        in
        let blockers = Hashtbl.fold  maybe_add_blocker locked_tlogs blocking_requests in
        if List.length blockers > 0 then
          Lwt_condition.wait blockers_cond >>= fun () ->
          self # wait_for_tlog_release tlog_file_n
        else
          Lwt.return ()

      method range ~consistency (first:string option) finc (last:string option) linc max =
        let start = Unix.gettimeofday() in
        self # _read_allowed consistency;
        self # _check_interval_range first last;
        let r = S.range store first finc last linc max in
        Statistics.new_range _stats start (Array.length r);
        r

      method private with_blocked_collapser start_i f =
        Lwt.finalize
          (fun () ->
             self # block_collapser start_i;
             f ()
          )
          (fun () ->
             let () = self # unblock_collapser start_i in
             Lwt.return ()
          )

      method last_entries (start_i:Sn.t) (oc:Lwt_io.output_channel) =
        self # with_blocked_collapser start_i
          (fun () ->
             Catchup.last_entries (module S) store tlog_collection start_i oc
          )

      method last_entries2 (start_i:Sn.t) (oc:Lwt_io.output_channel) =
        self # with_blocked_collapser start_i
          (fun () ->
             Catchup.last_entries2 (module S) store tlog_collection start_i oc
          )

      method range_entries ~consistency
               (first:string option) finc (last:string option) linc max =
        let start = Unix.gettimeofday() in
        self # _read_allowed consistency;
        self # _check_interval_range first last;
        let r  = S.range_entries store first finc last linc max in
        Statistics.new_range_entries _stats start (fst r);
        r

      method rev_range_entries ~consistency
               (first:string option) finc (last:string option) linc max =
        let start = Unix.gettimeofday() in
        self # _read_allowed consistency;
        self # _check_interval_range last first;
        let r = S.rev_range_entries store first finc last linc max in
        Statistics.new_rev_range_entries _stats start (fst r);
        r

      method prefix_keys ~consistency (prefix:string) (max:int) =
        let start = Unix.gettimeofday() in
        self # _read_allowed consistency;
        self # _check_interval [prefix];
        let key_list = S.prefix_keys store prefix max in
        let n_keys = fst key_list in
        Logger.ign_debug_f_ "prefix_keys found %d matching keys" n_keys;
        Statistics.new_prefix_keys _stats start n_keys;
        key_list

      method set key value =
        let start = Unix.gettimeofday () in
        self # _check_interval [key];
        let () = assert_value_size value in
        let update = Update.Set(key,value) in
        let update_sets (_update_result:Store.update_result) = Statistics.new_set _stats key value start in
        _update_rendezvous self update update_sets push_update ~so_post:_mute_so


      method nop () =
        let update = Update.Nop in
        _update_rendezvous self update no_stats push_update ~so_post:_mute_so

      method get_txid () =
        let io = S.consensus_i store in
        let i = match io with None -> Sn.zero | Some i -> i in
        At_least i

      method confirm key value =
        log_o self "confirm %S" key >>= fun () ->
        let () = assert_value_size value in
        self # _read_allowed Consistent;
        try
          let old_value = S.get store key in
          if old_value = value
          then Lwt.return ()
          else self # set key value
        with Not_found ->
          self # set key value

      method set_routing r =
        log_o self "set_routing" >>= fun () ->
        let update = Update.SetRouting r in
        _update_rendezvous self update no_stats push_update ~so_post:_mute_so

      method set_routing_delta left sep right =
        log_o self "set_routing_delta" >>= fun () ->
        let update = Update.SetRoutingDelta (left, sep, right) in
        _update_rendezvous self update no_stats push_update ~so_post:_mute_so

      method set_interval iv =
        log_o self "set_interval %s" (Interval.to_string iv)>>= fun () ->
        let update = Update.SetInterval iv in
        _update_rendezvous self update no_stats push_update ~so_post:_mute_so

      method user_function name po =
        log_o self "user_function %s" name >>= fun () ->
        if Registry.Registry.exists name
        then
          begin
            let update = Update.UserFunction(name,po) in
            let so_post so = so in
            _update_rendezvous self update no_stats push_update ~so_post
          end
        else
          Lwt.fail (XException(Arakoon_exc.E_BAD_INPUT, Printf.sprintf "User function %S could not be found" name))

      method get_read_user_db () =
        S.get_read_user_db store

      method push_update update =
        _update_rendezvous self update ignore push_update ~so_post:id

      method aSSert ~consistency (key:string) (vo:string option) =
        self # _read_allowed consistency;
        let vo' =
          try Some (S.get store key)
          with Not_found -> None in
        if vo' <> vo
        then raise (XException(Arakoon_exc.E_ASSERTION_FAILED,key))

      method aSSert_exists ~consistency (key:string)=
        self # _read_allowed consistency;
        let exist = S.exists store key in
        if not exist
        then raise (XException(Arakoon_exc.E_ASSERTION_FAILED,key))

      method test_and_set key expected (wanted:string option) =
        let start = Unix.gettimeofday() in
        log_o self "test_and_set %s %s %s" key
          (string_option2s expected)
          (string_option2s wanted)
        >>= fun () ->
        let () = match wanted with
          | None -> ()
          | Some w -> assert_value_size w
        in
        let update = Update.TestAndSet(key, expected, wanted) in
        let update_stats _ur = Statistics.new_testandset _stats start in
        let so_post so = so in
        _update_rendezvous self update update_stats push_update ~so_post

      method replace key (wanted:string option) =
        let start = Unix.gettimeofday() in
        log_o self "replace %s %s" key (string_option2s wanted) >>= fun () ->
        let () = match wanted with
          | None -> ()
          | Some w -> assert_value_size w
        in
        let update = Update.Replace(key,wanted) in
        let update_stats _ur = Statistics.new_replace _stats start in
        let so_post so = so in
        _update_rendezvous self update update_stats push_update ~so_post

      method delete_prefix prefix =
        let start = Unix.gettimeofday () in
        log_o self "delete_prefix %S" prefix >>= fun () ->
        (* do we need to test the prefix on the interval ? *)
        let update = Update.DeletePrefix prefix in
        let update_stats ur =
          let open Simple_store in
          let n_keys =
            match ur with
              | Ok so -> (match so with | None -> 0 | Some ns -> (Llio.int_from (Llio.make_buffer ns 0)))
              | Update_fail _ -> failwith  "how did I get here?" (* exception would be thrown BEFORE we reach this *)
          in
          Statistics.new_delete_prefix _stats start n_keys
        in
        let so_post = function
          | None -> 0
          | Some s -> Llio.int_from (Llio.make_buffer s 0)
        in
        _update_rendezvous self update update_stats push_update ~so_post


      method delete key = log_o self "delete %S" key >>= fun () ->
        let start = Unix.gettimeofday () in
        let update = Update.Delete key in
        let update_stats _ur = Statistics.new_delete _stats start in
        _update_rendezvous self update update_stats push_update ~so_post:_mute_so

      method hello (_client_id:string) (cluster_id:string) =
        if test ~cluster_id
        then
          let msg = Printf.sprintf
                      "Arakoon %i.%i.%i"
                      Arakoon_version.major Arakoon_version.minor Arakoon_version.patch
          in
          Lwt.return msg
        else
          Lwt.fail (Arakoon_exc.Exception (Arakoon_exc.E_WRONG_CLUSTER, "Wrong cluster"))

      method flush_store () =
        S.flush store


      method sequence ~sync (updates:Update.t list) =
        let start = Unix.gettimeofday() in
        let update = if sync
          then Update.SyncedSequence updates
          else Update.Sequence updates
        in
        let update_stats _ur = Statistics.new_sequence _stats start in
        let so_post _ = () in
        _update_rendezvous self update update_stats push_update ~so_post

      method multi_get ~consistency (keys:string list) =
        let start = Unix.gettimeofday() in
        self # _read_allowed consistency;
        let values = S.multi_get store keys in
        Statistics.new_multiget _stats start;
        values

      method multi_get_option ~consistency (keys:string list) =
        let start = Unix.gettimeofday() in
        self # _read_allowed consistency;
        let vos = S.multi_get_option store keys in
        Statistics.new_multiget_option _stats start;
        vos

      method to_string () = "sync_backend(" ^ (Node_cfg.node_name cfg) ^")"

      method private _who_master () =
        S.who_master store

      method who_master () =
        let mo = self # _who_master () in
        let result,argumentation =
          match mo with
            | None -> None,"young cluster"
            | Some (m,ls) ->
              match Node_cfg.get_master cfg with
                | Elected | Preferred _ | Forced _ ->
                  begin
                    let now = Unix.gettimeofday () in
                    let diff = now -. ls in
                    if diff < float lease_expiration then
                      (Some m,"inside lease")
                    else (None,Printf.sprintf "(%f < (%f = now) lease expired" ls now)
                  end
                | ReadOnly -> Some my_name, "readonly"

        in
        Logger.ign_debug_f_ "who_master: returning %s because %s"
          (Log_extra.string_option2s result) argumentation;
        result

      method private _not_if_master() =
        match self # who_master () with
        | None ->
          Lwt.return ()
        | Some m ->
          if m = my_name
          then
            let msg = "Operation cannot be performed on master node" in
            Lwt.fail (XException(Arakoon_exc.E_NOT_SUPPORTED, msg))
          else
            Lwt.return ()

      method _write_allowed () =
        if read_only
        then raise (XException(Arakoon_exc.E_READ_ONLY, my_name))
        else if n_nodes <> 1
        then
          begin
            match self # who_master () with
            | None ->
              raise (XException(Arakoon_exc.E_NOT_MASTER, "None"))
            | Some m ->
              if m <> my_name
              then raise (XException(Arakoon_exc.E_NOT_MASTER, m))
          end

      method private _read_allowed (consistency:consistency) =
        if not read_only
        then
          match consistency with
          | Consistent    -> self # _write_allowed ()
          | No_guarantees -> ()
          | At_least s    ->
             begin

               let io = S.consensus_i store in
               Logger.ign_debug_f_ "_read_allowed: store @ %s at_least=%s" (Log_extra.option2s Sn.string_of io) (Sn.string_of s);
               let i = match io with
                 | None -> Sn.zero
                 | Some i -> i
               in
               if Stamp.(<=) s i
               then Logger.ign_debug_f_ "ok"
               else raise (XException(Arakoon_exc.E_INCONSISTENT_READ, "store not fresh enough"))
             end

      method read_allowed consistency =
        self # _read_allowed consistency

      method private _check_interval keys =
        let iv = S.get_interval store in
        let rec loop = function
          | [] -> ()
          | k :: keys ->
            if Interval.is_ok iv k
            then loop keys
            else raise (XException(Arakoon_exc.E_OUTSIDE_INTERVAL, k))
        in
        loop keys

      method private _check_interval_range first last =
        let iv = S.get_interval store in
        let check_option = function
          | None -> ()
          | Some k ->
            if not (Interval.is_ok iv k)
            then raise (XException(Arakoon_exc.E_OUTSIDE_INTERVAL, k))
        in
        check_option first;
        check_option last

      method witness name i =
        Statistics.witness _stats name i;
        let cio = S.consensus_i store in
        begin
          match cio with
            | None -> ()
            | Some ci -> Statistics.witness _stats my_name  ci
        end;
        ()

      method last_witnessed name = Statistics.last_witnessed _stats name

      method expect_progress_possible () =
        match S.consensus_i store with
          | None -> false
          | Some i ->
            let q = quorum_function n_nodes in
            let count,s = Hashtbl.fold
                            (fun name ci (count,s) ->
                               let s' = s ^ Printf.sprintf " (%s,%s) " name (Sn.string_of ci) in
                               if (expect_reachable ~target:name) &&
                                  (ci = i || (Sn.pred ci) = i )
                               then  count+1,s'
                               else  count,s')
                            ( Statistics.get_witnessed _stats ) (1,"") in
            let v = count >= q in
            Logger.ign_debug_f_ "count:%i,q=%i i=%s detail:%s" count q (Sn.string_of i) s;
            v

      method get_statistics () =
        (* It's here and not in Statistics as we use statistics in the
           client library, and we don't want to depend on things like Limits
        *)
        let apply_latest t =
          let open Statistics in
          let maxrss = Limits.get_maxrss() in
          let stat = Gc.quick_stat () in
          let factor = float (Sys.word_size / 8) in
          let allocated = (stat.Gc.minor_words +. stat.Gc.major_words -. stat.Gc.promoted_words)
                          *. (factor /. 1024.0) in
          t.mem_allocated <- allocated;
          t.mem_maxrss <- maxrss;
          t.mem_minor_collections <- stat.Gc.minor_collections;
          t.mem_major_collections <- stat.Gc.major_collections;
          t.mem_compactions <- stat.Gc.compactions
        in
        apply_latest _stats;
        _stats

      method clear_most_statistics () = Statistics.clear_most _stats
      method check ~cluster_id =
        let r = test ~cluster_id in
        r

      method collapse n cb' cb =
        if n < 1 then
          let rc = Arakoon_exc.E_UNKNOWN_FAILURE
          and msg = Printf.sprintf "%i is not acceptable" n
          in
          Lwt.fail (XException(rc,msg))
        else
          Logger.debug_f_ "collapsing_lock locked: %s"
                          (string_of_bool (Lwt_mutex.is_locked collapsing_lock)) >>= fun () ->
          if Lwt_mutex.is_locked collapsing_lock then
            let rc = Arakoon_exc.E_UNKNOWN_FAILURE
            and msg = "Collapsing already in progress"
            in
            Lwt.fail (XException(rc,msg))
          else
            Lwt_mutex.with_lock
              collapsing_lock
              (fun () ->
               let new_cb tlog_num =
                 cb() >>= fun () ->
                 self # wait_for_tlog_release tlog_num
               in
               Logger.info_ "Starting collapse" >>= fun () ->
               Collapser.collapse_many tlog_collection (module S)
                                       store_methods n cb' new_cb
                                       collapse_slowdown >>= fun () ->
               Logger.info_ "Collapse completed")

      method get_routing () =
        self # _read_allowed Consistent;
        try
          S.get_routing store
        with
        | Store.CorruptStore ->
           begin
             Logger.ign_fatal_ "CORRUPT_STORE";
             raise Server.FOOBAR
           end
        | ext -> raise ext


      method get_key_count () =
        self # _read_allowed Consistent;
        S.get_key_count store

      method private quiesce_db ~mode () =
        self # _not_if_master () >>= fun () ->
        let sleep, awake = Lwt.wait() in
        let update = Multi_paxos.Quiesce (mode, sleep, awake) in
        Logger.info_ "quiesce_db: Pushing quiesce request" >>= fun () ->
        push_node_msg update >>= fun () ->
        Logger.info_ "quiesce_db: waiting for quiesce request to be completed" >>= fun () ->
        sleep >>= fun res ->
        Logger.info_ "quiesce_db: db is now completed" >>= fun () ->
        match res with
        | Quiesce.Result.OK -> Lwt.return ()
        | Quiesce.Result.FailMaster ->
          Lwt.fail (XException(Arakoon_exc.E_NOT_SUPPORTED, "Operation cannot be performed on master node"))
        | Quiesce.Result.Fail ->
          Lwt.fail (XException(Arakoon_exc.E_UNKNOWN_FAILURE, "Store could not be quiesced"))

      method private unquiesce_db () =
        act_not_preferred := false;
        Logger.info_ "unquiesce_db: Leaving quisced state" >>= fun () ->
        let update = Multi_paxos.Unquiesce in
        push_node_msg update

      method try_quiesced ~mode f =
        if is_witness
        then
          Lwt.fail (XException(Arakoon_exc.E_NOT_SUPPORTED, "Operation not supported on witness nodes"))
        else
          begin
            self # quiesce_db ~mode () >>= fun () ->
            begin
              Lwt.finalize
                ( f )
                ( self # unquiesce_db )
            end
          end

      method optimize_db () =
        Logger.info_ "optimize_db: enter" >>= fun () ->
        let mode = Quiesce.Mode.ReadOnly in
        self # try_quiesced ~mode (fun () -> Lwt.map ignore (S.optimize store)) >>= fun () ->
        Logger.info_ "optimize_db: All done"

      method defrag_db () =
        self # _not_if_master() >>= fun () ->
        Logger.info_ "defrag_db: enter" >>= fun () ->
        let mode = Quiesce.Mode.Writable in
        self # try_quiesced ~mode (fun () -> S.defrag store) >>= fun () ->
        Logger.info_ "defrag_db: exit"


      method get_db m_oc =
        Logger.info_ "get_db: enter" >>= fun () ->
        begin
          match m_oc with
            | None ->
              let ex = XException(Arakoon_exc.E_UNKNOWN_FAILURE, "Can only stream on a valid out channel") in
              Lwt.fail ex
            | Some oc -> Lwt.return oc
        end >>= fun oc ->
        let mode = Quiesce.Mode.ReadOnly in
        self # try_quiesced ~mode ( fun () -> S.copy_store store oc ) >>= fun () ->
        Logger.info_ "get_db: All done"

      method copy_db_to_head tlogs_to_keep =
        if tlogs_to_keep < 1 then
          let rc = Arakoon_exc.E_UNKNOWN_FAILURE
          and msg = Printf.sprintf "tlogs_to_keep=%i is not acceptable" tlogs_to_keep
          in
          Lwt.fail (XException(rc,msg))
        else
          begin
            let mode = Quiesce.Mode.ReadOnly in
            let head_dir = cfg.Node_cfg.head_dir in
            let head_path = Filename.concat head_dir Tlc2.head_fname in
            self # try_quiesced
                     ~mode
                     (fun () ->
                      S.copy_store2 (S.get_location store) head_path
                                    ~overwrite:true ~throttling:copy_head_throttling) >>= fun () ->

            (* remove all but tlogs_to_keep last tlogs *)
            Collapser._head_i (module S) head_path >>= fun head_io ->

            let head_n = match head_io with
              | None -> failwith "impossible i for copied head"
              | Some i -> Tlc2.get_file_number i
            in
            let keep_bottom_n = Sn.succ (Sn.sub head_n (Sn.of_int tlogs_to_keep)) in
            if Sn.compare keep_bottom_n Sn.zero = 1
            then
              begin
                self # wait_for_tlog_release keep_bottom_n >>= fun () ->
                tlog_collection # remove_below (Tlc2.get_tlog_i keep_bottom_n)
              end
            else
              Lwt.return ()
          end

      method get_cluster_cfgs () =
        begin
          match client_cfgs with
            | None ->
              let cfgs = S.range_entries
                           store ~_pf:Simple_store.__adminprefix
                           ncfg_prefix_b4_o false ncfg_prefix_2far_o false (-1) in
              let result = Hashtbl.create 5 in
              let add_item (item: Key.t*string) =
                let k,v = item in
                let cfg = ClientCfg.cfg_from (Llio.make_buffer v 0) in
                let start = String.length ncfg_prefix_b4 in
                let length = (Key.length k) - start in
                let k' = Key.sub k start length in
                Hashtbl.replace result k' cfg
              in
              List.iter add_item (snd cfgs);
              let () = client_cfgs <- (Some result) in
              result
            | Some res ->
              res
        end

      method set_cluster_cfg cluster_id cfg =
        let key = ncfg_prefix_b4 ^ cluster_id in
        let buf = Buffer.create 100 in
        ClientCfg.cfg_to buf cfg;
        let value = Buffer.contents buf in
        let update = Update.AdminSet (key,Some value) in
        _update_rendezvous self update no_stats push_update ~so_post:_mute_so >>= fun () ->
        begin
          match client_cfgs with
            | None ->
              let res = Hashtbl.create 5 in
              Hashtbl.replace res cluster_id cfg;
              Logger.debug_ "set_cluster_cfg creating new cached hashtbl" >>= fun () ->
              Lwt.return ( client_cfgs <- (Some res) )
            | Some res ->
              Logger.debug_ "set_cluster_cfg updating cached hashtbl" >>= fun () ->
              Lwt.return ( Hashtbl.replace res cluster_id cfg )
        end

      method get_fringe boundary direction =
        Logger.ign_debug_f_ "get_fringe %S" (Log_extra.string_option2s boundary);
        S.get_fringe store boundary direction

      method drop_master () =
        if n_nodes = 1
        then
          let e = XException(Arakoon_exc.E_NOT_SUPPORTED, "drop master not supported for singletons")in
          Lwt.fail e
        else
          begin
            match self # who_master () with
            | None -> Lwt.return ()
            | Some m ->
              if m <> my_name
              then Lwt.return ()
              else
                begin
                  act_not_preferred := true;
                  let (sleep, awake) = Lwt.wait () in
                  let update = Multi_paxos.DropMaster (sleep, awake) in
                  Logger.debug_ "drop_master: pushing update" >>= fun () ->
                  push_node_msg update >>= fun () ->
                  Logger.debug_ "drop_master: waiting for completion" >>= fun () ->
                  sleep >>= fun () ->
                  let delay = (float lease_expiration) *. 0.5 in
                  Logger.debug_f_ "drop_master: waiting another %1.1fs" delay >>= fun () ->
                  Lwt_unix.sleep delay >>= fun () ->
                  Logger.debug_ "drop_master: completed"
                end
          end

      method get_current_state () =
        Multi_paxos_fsm.pull_state ()
    end

end
