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

open Hotc
open Node_cfg.Node_cfg
open Tcp_messaging
open Update
open Lwt
open Lwt_buffer
open Tlogcommon
open Gc
open Master_type

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
  let acc = ref None in
  let f () c =
    if c.node_name = me 
    then acc := Some c
  in
  let () = List.fold_left f () cluster_cfg.cfgs in
  let cfg = 
  begin
    match !acc with 
    | Some c -> c
    | None -> failwith( "Could not find config for node " ^ me ) 
  end in
  let level =
    match cfg.log_level with
      | "info"    -> Lwt_log.Info
      | "notice"  -> Lwt_log.Notice
      | "warning" -> Lwt_log.Warning
      | "error"   -> Lwt_log.Error
      | "fatal"   -> Lwt_log.Fatal
      | _         -> Lwt_log.Debug
  in
  let log_dir = cfg.log_dir in
  let node_name = cfg.node_name in
  let common_prefix = log_dir ^ "/" ^ node_name in
  let log_file_name = common_prefix ^ ".log" in
  let get_crash_file_name () = Printf.sprintf "%s.debug.%f" common_prefix (Unix.time()) in
  if log_dir <> "none"
  then
    Some ( Crash_logger.setup_default_logger level log_file_name get_crash_file_name )
  else
    None

let _config_messaging me others cookie laggy =
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
  let mapping = List.map
    (fun cfg -> (cfg.node_name, (cfg.ip, cfg.messaging_port)))
    others
  in
  let messaging = new tcp_messaging 
    (me.ip, me.messaging_port) cookie drop_it in
    messaging # register_receivers mapping;
    (messaging :> Messaging.messaging)

open Mp_msg

let _check_tlogs collection tlog_dir =
  Lwt_log.info_f "checking tlog directory: %s" tlog_dir >>= fun () ->
  collection # validate_last_tlog () >>= fun (validity, lastI) ->
  begin
    match lastI with
      | None -> (Lwt_log.info_f "tlog is empty" )
      | Some i  -> Lwt_log.info_f "tlog's lastI = %s" (Sn.string_of i)
  end >>= fun () ->
  Lwt.return lastI

let _maybe_daemonize daemonize cfg get_cfgs =
  if daemonize then
    begin
      Lwt_daemon.daemonize
	~syslog:false
	~stdin:`Close
	~stdout:`Dev_null
	~stderr:`Dev_null
	();
      let _ = _config_logging cfg.node_name get_cfgs in
      Lwt.return()
    end
  else
    Lwt.return ()


let _config_service cfg backend=
  let port = cfg.client_port in
  let host = cfg.ip in
  let max_connections = 
    let hard = Limits.get_rlimit Limits.NOFILE Limits.Hard in
    max (hard -200) 200
  in
  Server.make_server_thread host port (Client_protocol.protocol backend) ~max_connections

let _log_rotate cfg i get_cfgs =
  Lwt_log.warning_f "received USR (%i) going to close/reopen log file" i
  >>= fun () ->
  let logger = !Lwt_log.default in
  let _ = _config_logging cfg get_cfgs in
  Lwt_log.close logger >>= fun () ->
  Lwt.return ()

let log_prelude() =
  Lwt_log.info "--- NODE STARTED ---" >>= fun () ->
  Lwt_log.info_f "hg_revision: %s " Version.hg_revision >>= fun () ->
  Lwt_log.info_f "compile_time: %s " Version.compile_time >>= fun () ->
  Lwt_log.info_f "version: %s" Version.version >>= fun () ->
  Lwt_log.info_f "NOFILE: %i" (Limits.get_rlimit Limits.NOFILE Limits.Hard) 
  >>= fun () ->
  Lwt_log.info_f "tlogEntriesPerFile: %i" (!Tlogcommon.tlogEntriesPerFile)


let full_db_name me = me.home ^ "/" ^ me.node_name ^ ".db" 

let only_catchup ~name ~cluster_cfg ~make_store ~make_tlog_coll = 
  Lwt_log.info "ONLY CATCHUP" >>= fun () ->
  let node_cnt = List.length cluster_cfg.cfgs in
  let me, other_configs = _split name cluster_cfg.cfgs in
  let cluster_id = cluster_cfg.cluster_id in
  let db_name = full_db_name me in
  begin
    if node_cnt > 1 then
      Catchup.get_db db_name cluster_id other_configs 
    else
      Lwt.return None
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
  make_store db_name >>= fun (store:Store.store) ->
  make_tlog_coll me.tlog_dir true >>= fun tlc ->
  let current_i = Sn.start in
  let future_n = Sn.start in
  let future_i = Sn.start in
  Catchup.catchup me other_configs ~cluster_id 
    (store,tlc)  current_i mr_name (future_n,future_i) 


module X = struct 
      (* Need to find a name for this: 
	 the idea is to lift stuff out of _main_2 
      *)
  
  let on_consensus store vni = 
    let (v,n,i) = vni in
    if Update.is_master_set v
    then begin store # incr_i () >>= fun () -> Lwt.return (Store.Ok None) end
    else Store.on_consensus store vni

  
  let on_accept tlog_coll store (v,n,i) =
    Lwt_log.debug_f "on_accept: n:%s i:%s" 
      (Sn.string_of n) (Sn.string_of i)
    >>= fun () ->
    let Value.V(update_string) = v in
    let u, _ = Update.from_buffer update_string 0 in
    tlog_coll # log_update i u >>= fun wr_result ->
    begin
      match u with
	| Update.MasterSet (m,l) ->
	  begin
	    let now = Int64.of_float (Unix.gettimeofday ()) in
	    store # set_master_no_inc m now >>= fun _ -> Lwt.return ()
	  end
	| _ -> Lwt.return()
    end 
    >>= fun () ->
    Lwt.return v

  let rapporting backend () = 
    let rec _inner () =
      Lwt_unix.sleep 300.0 >>= fun () ->
      let stats = backend # get_statistics () in
      Lwt_log.info_f "stats: %s" (Statistics.Statistics.string_of stats) 
      >>= fun () ->
      let () = backend # clear_statistics () in
      let sqs = Lwt_unix.sleep_queue_size () in
      let ns = Lwt_unix.get_new_sleeps () in
      let wcl = Lwt_unix.wait_children_length () in
      let stat = Gc.stat () in
      let factor = float (Sys.word_size / 8) in
      let allocated = (stat.minor_words +.
			 stat.major_words -. stat.promoted_words) *. 
	(factor /. 1024.0) 
      in
      Lwt_log.info_f "sleeping_queue_size=%i\nnew_sleeps=%i\nwait_children_length=%i\nallocated=%fKB" sqs ns wcl allocated 
      >>= fun () ->
      _inner ()
    in
    _inner ()
end

let _main_2 make_store make_tlog_coll make_config ~name 
    ~daemonize ~catchup_only=
  Lwt_io.set_default_buffer_size 32768;
  let control  = {
     minor_heap_size = 32 * 1024;
     major_heap_increment = 124 * 1024;
     space_overhead = 80;
     verbose = 0;
     max_overhead = 0;
     stack_limit = 256 * 1024;
     allocation_policy = 1; 
  } in
  Gc.set control;
  let dump_crash_log = ref None in
  let cluster_cfg = make_config () in
  let cfgs = cluster_cfg.cfgs in
  let me, others = _split name cfgs in
  dump_crash_log := _config_logging me.node_name make_config;
  if catchup_only 
  then 
    begin
      only_catchup ~name ~cluster_cfg ~make_store ~make_tlog_coll 
      >>= fun _ -> (* we don't need that here as there is no continuation *)
      Lwt.return ()
	
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
      let other_names = 
	if me.is_learner 
	then me.targets 
	else List.filter ((<>) name) in_cluster_names in
      let _ = Lwt_unix.on_signal 10
	(fun i -> Lwt.ignore_result (_log_rotate me.node_name i make_config ))
      in
      log_prelude() >>= fun () ->
      let my_name = me.node_name in
      let cookie = cluster_id in
      let messaging  = _config_messaging me cfgs cookie me.is_laggy in
      let build_startup_state () = 
	begin
	  Lwt_list.iter_s (Lwt_log.info_f "other: %s")
	    other_names >>= fun () ->
	  Lwt_log.info_f "master=%s" (master2s master) >>= fun () ->
	  Lwt_log.info_f "lease_period=%i" cluster_cfg._lease_period >>= fun () ->
	  Lwt_log.info_f "quorum_function gives %i for %i" 
	    (quorum_function n_nodes) n_nodes >>= fun () ->
	  Node_cfg.Node_cfg.validate_dirs me >>= fun () -> 
	  let db_name = full_db_name me in
	  Collapser.maybe_copy_head me.tlog_dir db_name >>= fun () ->
	  make_store db_name >>= fun (store:Store.store) ->
          Lwt.catch
	    ( fun () -> make_tlog_coll me.tlog_dir me.use_compression) 
	    ( function 
              | Tlc2.TLCCorrupt (pos,tlog_i) ->
                store # consensus_i () >>= fun store_i ->
                Tlc2.get_last_tlog me.tlog_dir >>= fun (last_c, last_tlog) ->
                let tlog_i = 
                  begin
		    match tlog_i with
		      | i when i = Sn.start -> (Sn.mul (Sn.of_int !Tlogcommon.tlogEntriesPerFile) (Sn.of_int last_c) )
		      | _ -> tlog_i 
                  end 
                in
                let s_i = 
                  begin
		    match store_i with
		      | Some i -> i
		      | None -> Sn.start
                  end
                in
                begin
                  Lwt_log.debug_f "store_i: '%s' tlog_i: '%s' Diff: %d" 
		    (Sn.string_of s_i) 
		    (Sn.string_of tlog_i)  
		    (Sn.compare s_i tlog_i)  >>= fun() ->
                  if (Sn.compare s_i tlog_i)  <= 0
                  then
		    begin
                      Lwt_log.warning_f "Invalid tlog file found. Auto-truncating tlog %s" 
			last_tlog >>= fun () ->
                      let _ = Tlc2.truncate_tlog last_tlog in
                      make_tlog_coll me.tlog_dir me.use_compression
		    end
                  else 
		    begin
                      Lwt_log.error_f "Store counter (%s) ahead of tlogs (%s). Aborting" 
			(Sn.string_of s_i) (Sn.string_of tlog_i) >>= fun () ->
                      Lwt.fail( Tlc2.TLCCorrupt (pos,tlog_i) )
		    end
                end
                  | ex -> Lwt.fail ex 
	    )
          >>= fun (tlog_coll:Tlogcollection.tlog_collection) ->
	  _check_tlogs tlog_coll me.tlog_dir >>= fun lastI ->
	  let current_i = match lastI with
	    | None -> Sn.start 
	    | Some i -> i
	  in
	  Catchup.verify_n_catchup_store me.node_name 
	    (store,tlog_coll,lastI) 
	    current_i master 
	  >>= fun (new_i, vo) ->
	  
	  let client_buffer =
	    let capacity = (Some 10) in
	    Lwt_buffer.create ~capacity () in
	  let client_push v = Lwt_buffer.add v client_buffer 
	  in
    let node_buffer = messaging # get_buffer my_name in
    let expect_reachable = messaging # expect_reachable in
    let inject_buffer = Lwt_buffer.create_fixed_capacity 1 in
    let read_only = master = ReadOnly in
    let inject_push v = Lwt_buffer.add v inject_buffer in
	  let sb =
	    let test = Node_cfg.Node_cfg.test cluster_cfg in
	    new Sync_backend.sync_backend me client_push inject_push
	      store tlog_coll lease_period
	      ~quorum_function n_nodes
	      ~expect_reachable
	      ~test
	      ~read_only
	  in
	  let backend = (sb :> Backend.backend) in

       	  let service = _config_service me backend in
	  
	  let send, receive, run, register =
	    Multi_paxos.network_of_messaging messaging in
	  
	  let on_consensus = X.on_consensus store in
	  let on_witness (name:string) (i: Sn.t) = backend # witness name i in
	  let last_witnessed (name:string) = backend # last_witnessed name in
	  let on_accept = X.on_accept tlog_coll store in
	  
	  let get_last_value (i:Sn.t) =
	    begin
	      tlog_coll # get_last_update i >>= fun uo ->
	      let r  = match uo with
		| None -> None
		| Some update -> Some (Update.make_update_value update)
	      in
	      Lwt.return r
	    end
	  in
	  let election_timeout_buffer = Lwt_buffer.create_fixed_capacity 1 in
	  let inject_event (e:Multi_paxos.paxos_event) =
	    let buffer,name = 
	      match e with
		| Multi_paxos.ElectionTimeout _ -> election_timeout_buffer, "election"
		| _ -> inject_buffer, "inject"
	    in
	    Lwt_log.debug_f "XXX injecting event into %s" name >>= fun () ->
	    Lwt_buffer.add e buffer
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
	      on_accept on_consensus 
	      on_witness
	      last_witnessed
	      (quorum_function: int -> int)
	      (master : master)
	      store tlog_coll others lease_period inject_event 
	      ~cluster_id

	  in Lwt.return ((master,constants, buffers, new_i, vo), 
			 service, X.rapporting backend)
	end
	  
      in

      let killswitch = Lwt_mutex.create () in
      Lwt_mutex.lock killswitch >>= fun () ->
      Lwt_unix.yield() >>= fun () ->
      let unlock_killswitch () = Lwt_mutex.unlock killswitch in
      let listen_for_sigterm () = Lwt_mutex.lock killswitch >>= fun () -> Lwt_unix.yield() in

      let start_backend (master, constants, buffers, new_i, vo) =
	let to_run = 
	  match master with
	    | Forced master  -> if master = my_name 
	      then Multi_paxos_fsm.enter_forced_master
	      else 
		begin
		  if me.is_learner 
		  then Multi_paxos_fsm.enter_simple_paxos
		  else Multi_paxos_fsm.enter_forced_slave 
		end
	    | ReadOnly -> Multi_paxos_fsm.enter_read_only
	    | _ -> Multi_paxos_fsm.enter_simple_paxos
	in to_run constants buffers new_i vo
      in
      Lwt_log.info_f "cfg = %s" (string_of me) >>= fun () ->
      _maybe_daemonize daemonize me make_config >>= fun _ ->
      Lwt_log.info_f "DAEMONIZATION=%b" daemonize >>= fun () ->
      Lwt.catch
	(fun () ->
          let _ = Lwt_unix.on_signal 15
            (fun i -> unlock_killswitch ())
          in
	  Lwt.pick [ 
	    messaging # run ();
	    begin
	      build_startup_state () >>= fun (start_state,
					      service, 
					      rapporting) -> 
	      Lwt.pick[ start_backend start_state;
			service ();
			rapporting ();
                        listen_for_sigterm ();
		      ]
	    end
	  ] >>= fun () ->
        Lwt_log.info "Completed shutdown"
        )
	(fun exn -> 
	  Lwt_log.fatal ~exn "going down" >>= fun () ->
	  match ! dump_crash_log with
	    | None -> Lwt_log.info "Not dumping state"
	    | Some f -> f()
	)
    end


let main_t make_config name daemonize catchup_only=
  let make_store = Local_store.make_local_store in
  let make_tlog_coll = Tlc2.make_tlc2
  in
  _main_2 make_store make_tlog_coll make_config ~name ~daemonize ~catchup_only

let test_t make_config name =
  let make_store = Mem_store.make_mem_store in
  let make_tlog_coll = Mem_tlogcollection.make_mem_tlog_collection in
  let daemonize = false 
  and catchup_only = false in
  _main_2 make_store make_tlog_coll make_config ~name ~daemonize ~catchup_only
