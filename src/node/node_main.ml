(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
*)

open Node_cfg.Node_cfg
open Tcp_messaging
open Update
open Lwt
open Lwt_buffer
open Tlogcommon
open Gc
open Master_type
open Client_cfg
open Statistics

let section = Logger.Section.main

let rec _split node_name cfgs =
  let rec loop me_o others = function
    | [] -> me_o, others
    | cfg :: rest ->
      if cfg.node_name = node_name then
        loop (Some cfg) others rest
      else
        loop me_o (cfg::others) rest
  in
  let me_o, others = loop None [] cfgs in
  match me_o with
    | None -> failwith (node_name ^ " is not known in config")
    | Some me -> me,others


let _config_logging me get_cfgs =
  let cluster_cfg = get_cfgs () in
  let cfg =
    try List.find (fun c -> c.node_name = me) cluster_cfg.cfgs
    with Not_found -> failwith ("Could not find config for node " ^ me )
  in
  let log_config =
    match cfg.log_config with
      | None -> Node_cfg.Node_cfg.get_default_log_config ()
      | Some log_config' ->
        try
          let (_, lc) = List.find (fun (log_config_name, log_config) -> log_config_name = log_config') cluster_cfg.log_cfgs in
          lc
        with Not_found ->
          failwith ("Could not find log section " ^ log_config' ^ " for node " ^ me)
  in
  let to_level = function
    | "info"    -> Logger.Info
    | "notice"  -> Logger.Notice
    | "warning" -> Logger.Warning
    | "error"   -> Logger.Error
    | "fatal"   -> Logger.Fatal
    | _         -> Logger.Debug
  in
  let level = to_level cfg.log_level in
  let () = Logger.Section.set_level Logger.Section.main level in
  let set_level section l =
    let l = match l with
      | None -> level
      | Some l -> to_level l in
    Logger.Section.set_level section l in
  let () = set_level Client_protocol.section log_config.client_protocol in
  let () = set_level Multi_paxos.section log_config.paxos in
  let () = set_level Tcp_messaging.section log_config.tcp_messaging in
  let log_dir = cfg.log_dir in
  let node_name = cfg.node_name in
  let common_prefix = log_dir ^ "/" ^ node_name in
  let log_file_name = common_prefix ^ ".log" in
  let get_crash_file_name () =
    Printf.sprintf "%s.debug.%f" common_prefix (Unix.time()) in
  if not cfg.is_test
  then
    begin
      Crash_logger.setup_default_logger log_file_name get_crash_file_name
      >>= fun logger -> Lwt.return (Some logger)
    end
  else
    begin
      Lwt.return None
    end

let _config_batched_transactions node_cfg cluster_cfg =
  let get_optional o default = match o with
    | None -> default
    | Some v -> v in
  let set_max e s =
    Batched_store.max_entries := get_optional e Batched_store.default_max_entries;
    Batched_store.max_size := get_optional s Batched_store.default_max_size;
  in
  match node_cfg.batched_transaction_config with
    | None ->
      set_max None None;
    | Some btc ->
      let (_, btc') =
        try
          List.find (fun (n,_) -> n = btc) cluster_cfg.batched_transaction_cfgs
        with exn -> failwith ("the batched_transaction_config section with name '" ^ btc ^ "' could not be found") in
      set_max btc'.max_entries btc'.max_size

let _config_messaging ?ssl_context me others cookie laggy lease_period max_buffer_size =
  let drop_it = match laggy with
    | true -> let count = ref 0 in
      let f msg source target =
        let () = incr count in
        match !count with
          | x when x >= 1000 -> let () = count := 0 in false
          | x when x >=  980 -> true
          | _ -> false
      in
      f
    | false -> (fun _ _ _ -> false)
  in
  let mapping =
    List.fold_left
      (fun acc cfg ->
         let a = List.map (fun ip -> (cfg.node_name, (ip, cfg.messaging_port))) cfg.ips in
         a @ acc)
      [] others
  in
  let client_ssl_context = ssl_context in
  let messaging = new tcp_messaging
    (me.ips, me.messaging_port) cookie drop_it max_buffer_size ~timeout:(lease_period *. 2.5)
    ?client_ssl_context in
  messaging # register_receivers mapping;
  (messaging :> Messaging.messaging)

open Mp_msg


let _config_service ?ssl_context cfg backend=
  let port = cfg.client_port in
  let hosts = cfg.ips in
  let max_connections =
    let soft = Limits.get_rlimit Limits.NOFILE Limits.Soft in
    max (soft -200) 200
  in
  let scheme = Server.create_connection_allocation_scheme max_connections in
  let services = List.map
                   (fun host ->
                      let name = Printf.sprintf "%s:client_service" host in
                      Server.make_server_thread ~name host port
                        (Client_protocol.protocol backend)
                        ~scheme ?ssl_context
                   )
                   hosts
  in
  let uber_service () = Lwt_list.iter_p (fun f -> f ()) services in
  uber_service

let _log_rotate cfg i get_cfgs =
  Logger.warning_f_ "received USR1 (%i) going to close/reopen log file" i
  >>= fun () ->
  let logger = !Lwt_log.default in
  _config_logging cfg get_cfgs >>= fun _ ->
  Lwt_log.close logger >>= fun () ->
  Lwt.return ()

let log_prelude cluster_cfg =
  Logger.info_ "--- NODE STARTED ---" >>= fun () ->
  Logger.info_f_ "git_revision: %s " Arakoon_version.git_revision >>= fun () ->
  Logger.info_f_ "compile_time: %s " Arakoon_version.compile_time >>= fun () ->
  Logger.info_f_ "version: %i.%i.%i" Arakoon_version.major
    Arakoon_version.minor Arakoon_version.patch   >>= fun () ->
  Logger.info_f_ "NOFILE: %i" (Limits.get_rlimit Limits.NOFILE Limits.Soft)      >>= fun () ->
  Logger.info_f_ "tlogEntriesPerFile: %i" (!Tlogcommon.tlogEntriesPerFile)       >>= fun () ->
  Logger.info_f_ "cluster_cfg=%s" (string_of_cluster_cfg cluster_cfg)            >>= fun () ->
  Logger.info_f_ "Batched_store.max_entries = %i" !Batched_store.max_entries    >>= fun () ->
  Logger.info_f_ "Batched_store.max_size = %i" !Batched_store.max_size


let full_db_name me = me.home ^ "/" ^ me.node_name ^ ".db"

let only_catchup (type s) (module S : Store.STORE with type t = s) ~name ~cluster_cfg ~make_tlog_coll =
  Logger.info_ "ONLY CATCHUP" >>= fun () ->
  let node_cnt = List.length cluster_cfg.cfgs in
  let me, other_configs = _split name cluster_cfg.cfgs in
  let cluster_id = cluster_cfg.cluster_id in
  let db_name = full_db_name me in
  begin
    if node_cnt > 1
    then Catchup.get_db db_name cluster_id other_configs
    else Lwt.return None
  end
  >>= fun m_mr_name ->
  let mr_name =
    begin
      match m_mr_name with
        | Some m -> m
        | None ->
          let fo = List.hd other_configs in
          fo.node_name
    end
  in
  S.make_store ~lcnum:cluster_cfg.lcnum
    ~ncnum:cluster_cfg.ncnum
    db_name >>= fun store ->
  make_tlog_coll me.tlog_dir me.tlf_dir me.head_dir
    me.use_compression me.fsync name >>= fun tlc ->
  Lwt.finalize
    (fun () ->
       let current_i = match S.consensus_i store with
         | None -> Sn.start
         | Some i -> i
       in
       let future_n = Sn.start in
       let future_i = Sn.start in
       Catchup.catchup ~stop:(ref false) me.Node_cfg.Node_cfg.node_name other_configs ~cluster_id
         ((module S),store,tlc)  current_i mr_name (future_n,future_i) >>= fun _ ->
       S.close store)
    (tlc # close)


let get_ssl_cert_paths me cluster =
  match me.tls_cert with
    | None -> None
    | Some tls_cert ->
        match me.tls_key with
          | None -> failwith "get_ssl_cert_paths: no tls_key"
          | Some tls_key ->
              match cluster.tls_ca_cert with
                | None -> failwith "get_ssl_cert_paths: no tls_ca_cert"
                | Some tls_ca_cert -> Some (tls_cert, tls_key, tls_ca_cert)

let build_ssl_context me cluster =
  match get_ssl_cert_paths me cluster with
    | None -> None
    | Some (tls_cert, tls_key, tls_ca_cert) ->
        let ctx = Typed_ssl.create_both_context Ssl.TLSv1 in
        Typed_ssl.use_certificate ctx tls_cert tls_key;
        Typed_ssl.set_verify
          ctx
          [Ssl.Verify_fail_if_no_peer_cert]
          (Some Ssl.client_verify_callback);
        Typed_ssl.set_client_CA_list_from_file ctx tls_ca_cert;
        Typed_ssl.load_verify_locations ctx tls_ca_cert "";
        Some ctx

let build_service_ssl_context me cluster =
  if not cluster.tls_service
  then None
  else match get_ssl_cert_paths me cluster with
    | None -> failwith "build_service_ssl_context: tls_service but no cert/key paths"
    | Some (tls_cert, tls_key, tls_ca_cert) ->
        let ctx = Typed_ssl.create_server_context Ssl.TLSv1 in
        Typed_ssl.use_certificate ctx tls_cert tls_key;
        let verify =
          if cluster.tls_service_validate_peer
          then [Ssl.Verify_peer; Ssl.Verify_fail_if_no_peer_cert]
          else []
        in
        Typed_ssl.set_verify ctx verify (Some Ssl.client_verify_callback);
        Typed_ssl.set_client_CA_list_from_file ctx tls_ca_cert;
        Typed_ssl.load_verify_locations ctx tls_ca_cert "";
        Some ctx

module X = struct
  (* Need to find a name for this:
     the idea is to lift stuff out of _main_2
  *)

  let last_master_log_stmt = ref 0L

  let on_consensus (type s) (module S : Store.STORE with type t = s) store vni =
    let (v,n,i) = vni in
    begin
      let t0 = Unix.gettimeofday() in
      let v' = Value.fill_if_master_set v in
      let vni' = v',n,i in
      begin
        match v' with
          | Value.Vm (m, _) ->
            let now = Int64.of_float (Unix.gettimeofday ()) in
            let m_old_master = S.who_master store in
            let new_master =
              begin
                match m_old_master with
                  | Some(m_old,_) -> m <> m_old
                  | None -> true
              end
            in
            if (Int64.sub now !last_master_log_stmt >= 60L) || new_master then
              begin
                last_master_log_stmt := now;
                Logger.info_f_ "%s is master"  m
              end
            else
              Lwt.return ()
          | _ -> Lwt.return ()
      end >>= fun () ->
      S.on_consensus store vni' >>= fun r ->
      let t1 = Unix.gettimeofday () in
      let d = t1 -. t0 in
      Logger.debug_f_ "T:on_consensus took: %f" d  >>= fun () ->
      Lwt.return r
    end

  let on_accept (type s) statistics (tlog_coll:Tlogcollection.tlog_collection) (module S : Store.STORE with type t = s) store (v,n,i) =
    let t0 = Unix.gettimeofday () in
    Logger.debug_f_ "on_accept: n:%s i:%s" (Sn.string_of n) (Sn.string_of i)
    >>= fun () ->
    let sync = Value.is_synced v in
    let marker = (None:string option) in
    tlog_coll # log_value_explicit i v sync marker >>= fun wr_result ->
    begin
      match v with
        | Value.Vc (us,_)     ->
          let size = List.length us in
          let () = Statistics.new_harvest statistics size in
          Lwt.return ()
        | _ -> Lwt.return ()
    end  >>= fun () ->
    let t1 = Unix.gettimeofday() in
    let d = t1 -. t0 in
    Logger.debug_f_ "T:on_accept took: %f" d

  let reporting period backend () =
    let fp = float period in
    let rec _inner () =
      Lwt_unix.sleep fp >>= fun () ->
      let stats = backend # get_statistics () in
      Logger.info_f_ "stats: %s" (Statistics.string_of stats)
      >>= fun () ->
      backend # clear_most_statistics();
      _inner ()
    in
    _inner ()
end

let _main_2 (type s)
      (module S : Store.STORE with type t = s)
      make_tlog_coll make_config get_snapshot_name ~name
      ~daemonize ~catchup_only stop : int Lwt.t =
  Lwt_io.set_default_buffer_size 32768;
  let control  = {
    minor_heap_size = 32 * 1024;
    major_heap_increment = 124 * 1024;
    space_overhead = 80;
    verbose = 0;
    max_overhead = 0;
    stack_limit = 256 * 1024;
    allocation_policy = 1;
  }
  in
  Gc.set control;
  let cluster_cfg = make_config () in
  let cfgs = cluster_cfg.cfgs in
  let me, others = _split name cfgs in
  _config_logging me.node_name make_config >>= fun dump_crash_log ->
  let _ = Lwt_unix.on_signal Sys.sigusr2 (fun _ ->
      let handle () =
        Lwt_unix.sleep 0.001 >>= fun () ->
        Logger.info_ "Received USR2, dumping crash_log" >>= fun () ->
        match dump_crash_log with
          | None -> Logger.info_ "No crash_log defined"
          | Some f -> f () >>= fun () -> Logger.info_ "Crash log dumped"
      in
      Lwt.ignore_result (handle ())) in
  _config_batched_transactions me cluster_cfg;

  let ssl_context = build_ssl_context me cluster_cfg in
  let service_ssl_context = build_service_ssl_context me cluster_cfg in

  if catchup_only
  then
    begin
      only_catchup (module S) ~name ~cluster_cfg ~make_tlog_coll
      >>= fun _ -> (* we don't need that here as there is no continuation *)

      Lwt.return 0
    end
  else
    begin
      let cluster_id = cluster_cfg.cluster_id in
      let master = cluster_cfg._master in
      let lease_period = cluster_cfg._lease_period in
      let quorum_function = cluster_cfg.quorum_function in
      let in_cluster_cfgs = List.filter (fun cfg -> not cfg.is_learner ) cfgs in
      let in_cluster_names = List.map (fun cfg -> cfg.node_name) in_cluster_cfgs in
      let n_nodes = List.length in_cluster_names in

      let () = match cluster_cfg.overwrite_tlog_entries with
        | None -> ()
        | Some i ->  Tlogcommon.tlogEntriesPerFile := i
      in

      let my_clicfg =
        begin
          let ccfg = ClientCfg.make () in
          let add_one cfg =
            let node_address = cfg.ips, cfg.client_port in
            ClientCfg.add ccfg cfg.node_name node_address
          in
          List.iter add_one in_cluster_cfgs;
          ccfg
        end
      in
      let other_names =
        if me.is_learner
        then me.targets
        else List.filter ((<>) name) in_cluster_names
      in
      let _ = Lwt_unix.on_signal 10
                (fun i -> Lwt.ignore_result (_log_rotate me.node_name i make_config ))
      in
      log_prelude cluster_cfg >>= fun () ->
      Plugin_loader.load me.home cluster_cfg.plugins >>= fun () ->
      let my_name = me.node_name in
      let cookie = cluster_id in
      let rec upload_cfg_to_keeper () =
        begin
          match cluster_cfg.nursery_cfg with
            | None -> Logger.info_ "Cluster not part of nursery."
            | Some (n_cluster_id, cfg) ->
              begin
                let attempt_send success node_id =
                  match success with
                    | true -> Lwt.return true
                    | false ->
                      begin
                        let (ips, port) = ClientCfg.get cfg node_id in
                        let ipss = Log_extra.list2s (fun s -> s) ips in
                        Logger.debug_f_ "upload_cfg_to_keeper (%s,%i)" ipss port >>= fun () ->
                        Lwt.catch
                          (fun () ->
                             let ip0 = List.hd ips in
                             let address = Network.make_address ip0 port in
                             let upload connection =
                               Remote_nodestream.make_remote_nodestream n_cluster_id connection >>= fun (client) ->
                               client # store_cluster_cfg cluster_id my_clicfg
                             in
                             Lwt_io.with_connection address upload >>= fun () ->
                             Logger.info_f_ "Successfully uploaded config to nursery node %s" node_id >>= fun () ->
                             Lwt.return true
                          )
                          (fun e ->
                             let exc_msg = Printexc.to_string e in
                             Logger.warning_f_ "Attempt to upload config to %s failed (%s)" node_id exc_msg
                             >>= fun () ->
                             Lwt.return false
                          )
                      end
                in
                let node_names = ClientCfg.node_names cfg in
                Lwt_list.fold_left_s attempt_send false node_names >>= fun succeeded ->
                begin
                  match succeeded with
                    | true -> Lwt.return ()
                    | false -> Lwt_unix.sleep 5.0 >>= fun () -> upload_cfg_to_keeper ()
                end

              end
        end
      in
      Lwt.ignore_result ( upload_cfg_to_keeper () ) ;
      let messaging  = _config_messaging ?ssl_context me cfgs cookie me.is_laggy (float me.lease_period) cluster_cfg.max_buffer_size in
      Logger.info_f_ "cfg = %s" (string_of me) >>= fun () ->
      Lwt_list.iter_s (fun m -> Logger.info_f_ "other: %s" m)
        other_names >>= fun () ->
      Logger.info_f_ "quorum_function gives %i for %i"
        (quorum_function n_nodes) n_nodes >>= fun () ->
      Logger.info_f_ "DAEMONIZATION=%b" daemonize >>= fun () ->

      let build_startup_state () =
        begin
          Node_cfg.Node_cfg.validate_dirs me >>= fun () ->
          let db_name = full_db_name me in
          let snapshot_name = get_snapshot_name() in
          let full_snapshot_path = Filename.concat me.head_dir snapshot_name in
          Lwt.catch
            (fun () ->
               S.copy_store2 full_snapshot_path db_name false
            )
            (function
              | Not_found -> Lwt.return ()
              | e -> raise e
            )
          >>= fun () ->
          make_tlog_coll me.tlog_dir me.tlf_dir me.head_dir me.use_compression me.fsync name
          >>= fun (tlog_coll:Tlogcollection.tlog_collection) ->
          let lcnum = cluster_cfg.lcnum
          and ncnum = cluster_cfg.ncnum
          in
          S.make_store ~lcnum ~ncnum db_name >>= fun (store:S.t) ->
          let last_i = tlog_coll # get_last_i () in
          let ti_o = Some last_i in
          let current_i = (* confusingly ~current_i further in the callstack is too_far_i *)
            if n_nodes = 1
            then
              (* this is the only node, so accepting a value equals reaching consensus on it *)
              Sn.succ last_i
            else
              last_i in
          begin
            if me.is_forced_slave
            then
              begin
                S.quiesce store >>= fun () ->
                if last_i <> 0L
                then (* this is an optimization so catchup doesn't need to pretend
                        to apply all previous values from tlog to the store
                        if last_i = 0 this would result in an invalid store counter -1 *)
                  S._set_i store (Sn.pred last_i);
                Lwt.return ()
              end
            else
              Lwt.return ()
          end >>= fun () ->
          Lwt.catch
            (fun () ->
               Catchup.verify_n_catchup_store ~stop:(ref false) me.node_name
                 ((module S), store, tlog_coll, ti_o)
                 ~current_i master)
            (fun ex ->
               tlog_coll # close () >>= fun () ->
               Lwt.fail ex)
          >>= fun () ->
          S.clear_self_master store me.node_name;
          let new_i = S.get_succ_store_i store in
          let client_buffer =
            let capacity = Some (cluster_cfg.client_buffer_capacity) in
            Lwt_buffer.create ~capacity () in
          let client_push v = Lwt_buffer.add v client_buffer in
          let node_buffer = messaging # get_buffer my_name in
          let expect_reachable = messaging # expect_reachable in
          let inject_buffer = Lwt_buffer.create_fixed_capacity 1 in
          let inject_push v = Lwt_buffer.add v inject_buffer in
          let read_only = master = ReadOnly in
          let module SB = Sync_backend.Sync_backend(S) in
          let sb =
            let test = Node_cfg.Node_cfg.test cluster_cfg in
            new SB.sync_backend me
              (client_push: (Update.t * (Store.update_result -> unit Lwt.t)) -> unit Lwt.t)
              inject_push
              store (S.copy_store2, full_snapshot_path)
              tlog_coll lease_period
              ~quorum_function n_nodes
              ~expect_reachable
              ~test
              ~read_only
              ~max_value_size:cluster_cfg.max_value_size
          in
          let backend = (sb :> Backend.backend) in

          let service = _config_service ?ssl_context:service_ssl_context me backend in

          let send, receive, run, register =
            Multi_paxos.network_of_messaging messaging in

          let on_consensus = X.on_consensus (module S) store in
          let on_witness (name:string) (i: Sn.t) = backend # witness name i in
          let last_witnessed (name:string) = backend # last_witnessed name in
          let statistics = backend # get_statistics () in
          let on_accept = X.on_accept statistics tlog_coll (module S) store in

          let get_last_value (i:Sn.t) = tlog_coll # get_last_value i in
          let election_timeout_buffer = Lwt_buffer.create_fixed_capacity 1 in
          let inject_event (e:Multi_paxos.paxos_event) =
            let add_to_buffer,name =
              match e with
                | Multi_paxos.ElectionTimeout _ ->
                  (fun () -> Lwt_buffer.add e election_timeout_buffer), "election"
                | Multi_paxos.FromClient fc ->
                  (fun () ->
                     Lwt_list.iter_s
                       (fun u -> Lwt_buffer.add u client_buffer)
                       fc),
                  "client_buffer"
                | _ ->
                  (fun () -> Lwt_buffer.add e inject_buffer), "inject"
            in
            Logger.debug_f Multi_paxos.section "XXX injecting event %s into '%s'"
              (Multi_paxos.paxos_event2s e)
              name
            >>= fun () ->
            add_to_buffer () >>= fun () ->
            Logger.debug_f Multi_paxos.section "XXX injected event into '%s'" name
          in
          let buffers = Multi_paxos_fsm.make_buffers
                          (client_buffer,
                           node_buffer,
                           inject_buffer,
                           election_timeout_buffer)
          in
          let constants =
            Multi_paxos.make my_name
              me.is_learner
              other_names send receive
              get_last_value
              on_accept
              on_consensus
              on_witness
              last_witnessed
              (quorum_function: int -> int)
              (master : master)
              (module S)
              store
              tlog_coll
              others
              lease_period
              inject_event
              ~cluster_id
              false
              stop
          in
          let reporting_period = me.reporting in
          Lwt.return ((master,constants, buffers, new_i, store),
                      service, X.reporting reporting_period backend)
        end

      in

      let killswitch = Lwt_mutex.create () in
      Lwt_mutex.lock killswitch >>= fun () ->
      let unlock_killswitch (_:int) = Lwt_mutex.unlock killswitch in
      let listen_for_signal () = Lwt_mutex.lock killswitch in

      let start_backend stop (master, constants, buffers, new_i, store) =
        let to_run =
          if me.is_forced_slave
          then
            Multi_paxos_fsm.enter_forced_slave
          else
            match master with
              | Forced master  ->
                if master = my_name
                then Multi_paxos_fsm.enter_forced_master
                else
                  begin
                    if me.is_learner
                    then Multi_paxos_fsm.enter_simple_paxos
                    else Multi_paxos_fsm.enter_forced_slave
                  end
              | _ -> Multi_paxos_fsm.enter_simple_paxos
        in
        to_run ~stop constants buffers new_i
      in
      (*_maybe_daemonize daemonize me make_config >>= fun _ ->*)
      Lwt.catch
        (fun () ->
           let _ = Lwt_unix.on_signal 15 unlock_killswitch in (* TERM aka kill   *)
           let _ = Lwt_unix.on_signal 2  unlock_killswitch in (*  INT aka Ctrl-C *)
           build_startup_state () >>= fun (start_state,
                                           service,
                                           rapporting) ->
           let (_,constants,_,_,store) = start_state in
           let log_exception m t =
             Lwt.catch
               t
               (fun exn ->
                  Logger.fatal_ ~exn m >>= fun () ->
                  Lwt.fail exn) in
           let swallow_canceled t =
             Lwt.catch
               t
               (function
                 | Canceled -> Lwt.return ()
                 | exn -> Lwt.fail exn) in
           let stop_mvar = Lwt_mvar.create_empty () in
           let fsm () = start_backend stop start_state in
           let fsm_mutex = Lwt_mutex.create () in
           let fsm_t =
             swallow_canceled
               (fun () ->
                 log_exception
                   "Exception in fsm thread"
                   (fun () -> Lwt_mutex.with_lock fsm_mutex fsm)) in
           let msg_mutex = Lwt_mutex.create () in
           let msg_t =
             log_exception
               "Exception in messaging thread"
               (fun () -> Lwt_mutex.with_lock msg_mutex (messaging # run ?ssl_context)) in
           Lwt.finalize
             (fun () ->
                Lwt.choose
                  [Lwt.pick [
                      fsm_t;
                      msg_t;
                      service ();
                      rapporting ();
                      (listen_for_signal () >>= fun () ->
                       let msg = "got TERM | INT" in
                       Logger.info_ msg >>= fun () ->
                       Lwt_io.printl msg >>= fun () ->
                       Lwt_log.Section.set_level Lwt_log.Section.main Lwt_log.Debug;
                       List.iter
                         (fun n ->
                            let s = Lwt_log.Section.make n in
                            Lwt_log.Section.set_level s Lwt_log.Debug)
                         ["client_protocol"; "tcp_messaging"; "paxos"];
                       Logger.info_ "All logging set to debug level after TERM/INT"
                      );
                      Lwt_mvar.take stop_mvar];
                   fsm_t;])
             (fun () ->
                Lwt_mvar.put stop_mvar () >>= fun () ->
                stop := true;
                Logger.info_ "waiting for fsm and messaging thread to finish" >>= fun () ->
                Lwt.join [(Lwt_mutex.lock fsm_mutex >>= fun () ->
                           Logger.info_ "fsm thread finished");
                          (Lwt_mutex.lock msg_mutex >>= fun () ->
                           Logger.info_ "messaging thread finished")] >>= fun () ->
                let count_thread m =
                  let rec inner i =
                    Logger.info_f_ m i >>= fun () ->
                    Lwt_unix.sleep 1.0 >>= fun () ->
                    inner (succ i) in
                  inner 0 in
                Lwt.pick [ S.close ~flush:false store ;
                           count_thread "Closing store (%is)" ] >>= fun () ->
                Logger.fatal_f_
                  ">>> Closing the store @ %S succeeded: everything seems OK <<<"
                  (S.get_location store) >>= fun () ->
                Lwt.pick [ constants.Multi_paxos.tlog_coll # close () ;
                           count_thread "Closing tlog (%is)" ])
           >>= fun () ->
           Logger.info_ "Completed shutdown"
           >>= fun () ->
           Lwt.return 0
        )
        (function
          | Catchup.StoreAheadOfTlogs(s_i, tlog_i) ->
            let rc = 40 in
            Logger.fatal_f_ "[rc=%i] Store ahead of tlogs: s_i=%s, tlog_i=%s"
              rc (Sn.string_of s_i) (Sn.string_of tlog_i) >>= fun () ->
            Lwt.return rc
          | Catchup.StoreCounterTooLow msg ->
            let rc = 41 in
            Logger.fatal_f_ "[rc=%i] Store counter too low: %s" rc msg >>= fun () ->
            Lwt.return rc
          | Tlc2.TLCNotProperlyClosed msg ->
            let rc = 42 in
            Logger.fatal_f_ "[rc=%i] tlog not properly closed %s" rc msg >>= fun () ->
            Lwt.return rc
          | Node_cfg.InvalidTlogDir dir ->
            let rc = 43 in
            Logger.fatal_f_ "[rc=%i] Missing or inaccessible tlog directory: %s" rc dir >>= fun () ->
            Lwt.return rc
          | Node_cfg.InvalidHomeDir dir ->
            let rc = 44 in
            Logger.fatal_f_ "[rc=%i] Missing or inaccessible home directory: %s" rc dir >>= fun () ->
            Lwt.return rc
          | Local_store.BdbFFatal db ->
            let rc = 45 in
            Logger.fatal_f_ "[rc=%i] BDBFFATAL flag set on database %s" rc db >>= fun () ->
            Lwt.return rc
          | Node_cfg.InvalidTlfDir dir ->
            let rc = 46 in
            Logger.fatal_f_ "[rc=%i] Missing or inaccessible tlf directory: %s" rc dir >>= fun () ->
            Lwt.return rc
          | Node_cfg.InvalidHeadDir dir ->
            let rc = 47 in
            Logger.fatal_f_ "[rc=%i] Missing or inaccessible head directory: %s" rc dir >>= fun () ->
            Lwt.return rc
          | Tlogcommon.TLogUnexpectedEndOfFile pos ->
            let rc = 48 in
            Logger.fatal_f_ "[rc=%i] Unexpectedly reached the end of the tlog, last valid pos = %Li" rc pos >>= fun () ->
            Lwt.return rc
          | Tlogcommon.TLogCheckSumError pos ->
            let rc = 49 in
            Logger.fatal_f_ "[rc=%i] Tlog has a checksum error, last valid pos = %Li" rc pos >>= fun () ->
            Lwt.return rc
          | exn ->
            begin
              Logger.fatal_ ~exn "going down" >>= fun () ->
              Logger.fatal_ "after pick" >>= fun() ->
              begin
                match dump_crash_log with
                  | None -> Logger.info_ "Not dumping state"
                  | Some f -> f()
              end >>= fun () ->
              Lwt.return 1
            end
        )
    end


let main_t make_config name daemonize catchup_only : int Lwt.t =
  let module S = (val (Store.make_store_module (module Batched_store.Local_store))) in
  let make_tlog_coll = Tlc2.make_tlc2 in
  let get_snapshot_name = Tlc2.head_name in
  _main_2 (module S) make_tlog_coll make_config get_snapshot_name ~name ~daemonize ~catchup_only (ref false)

let test_t make_config name stop =
  let module S = (val (Store.make_store_module (module Mem_store))) in
  let make_tlog_coll = fun a b _ d -> Mem_tlogcollection.make_mem_tlog_collection a b d in
  let get_snapshot_name = fun () -> "DUMMY" in
  let daemonize = false
  and catchup_only = false in
  _main_2 (module S) make_tlog_coll make_config get_snapshot_name ~name ~daemonize ~catchup_only stop
