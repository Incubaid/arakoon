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
  let get_crash_file_name () = 
    Printf.sprintf "%s.debug.%f" common_prefix (Unix.time()) in
  if not cfg.is_test 
  then
    begin 
      Crash_logger.setup_default_logger level log_file_name get_crash_file_name 
      >>= fun logger -> Lwt.return (Some logger)
    end
  else
    begin
      Lwt.return None
    end
      
let _config_messaging me others cookie laggy lease_period max_buffer_size =
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
  let messaging = new tcp_messaging 
    (me.ips, me.messaging_port) cookie drop_it max_buffer_size ~timeout:lease_period in
  messaging # register_receivers mapping;
  (messaging :> Messaging.messaging)
    
open Mp_msg
    
    
let _config_service cfg backend=
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
        ~scheme
    )
    hosts
  in
  let uber_service () = Lwt_list.iter_p (fun f -> f ()) services in
  uber_service
    
let _log_rotate cfg i get_cfgs =
  Lwt_log.warning_f "received USR1 (%i) going to close/reopen log file" i
  >>= fun () ->
  let logger = !Lwt_log.default in
  _config_logging cfg get_cfgs >>= fun _ ->
  Lwt_log.close logger >>= fun () ->
  Lwt.return ()
    
let log_prelude cluster_cfg =
  Lwt_log.info "--- NODE STARTED ---" >>= fun () ->
  Lwt_log.info_f "hg_revision: %s " Version.hg_revision >>= fun () ->
  Lwt_log.info_f "compile_time: %s " Version.compile_time >>= fun () ->
  Lwt_log.info_f "version: %i.%i.%i" Version.major Version.minor Version.patch >>= fun () ->
  Lwt_log.info_f "NOFILE: %i" (Limits.get_rlimit Limits.NOFILE Limits.Soft) 
  >>= fun () ->
  Lwt_log.info_f "tlogEntriesPerFile: %i" (!Tlogcommon.tlogEntriesPerFile)
  >>= fun () ->
  Lwt_log.info_f "max_value_size: %i" cluster_cfg.max_value_size 
  >>= fun () ->
  Lwt_log.info_f "max_buffer_size: %i" cluster_cfg.max_buffer_size
  >>= fun () ->
  let ncfgo = cluster_cfg.nursery_cfg in
  let p2s (nc,cfg) =  Printf.sprintf "(%s,%s)" nc (ClientCfg.to_string cfg) in
  let ccfg_s = Log_extra.option2s p2s ncfgo in
  Lwt_log.info_f "client_cfg=%s" ccfg_s
    

let full_db_name me = me.home ^ "/" ^ me.node_name ^ ".db" 

let only_catchup ~name ~cluster_cfg ~make_store ~make_tlog_coll = 
  Lwt_log.info "ONLY CATCHUP" >>= fun () ->
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
  make_store db_name >>= fun (store:Store.store) ->
  make_tlog_coll me.tlog_dir me.use_compression >>= fun tlc ->
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
    begin
      if Value.is_master_set v
      then 
	    begin
	      store # incr_i () >>= fun () -> Lwt.return (Store.Ok None) 
	    end
      else
	    Store.on_consensus store vni
    end
  
  let last_master_log_stmt = ref 0L  
    
  let on_accept tlog_coll store (v,n,i) =
    Lwt_log.debug_f "on_accept: n:%s i:%s" (Sn.string_of n) (Sn.string_of i) 
    >>= fun () ->
    let sync = Value.is_synced v in
    tlog_coll # log_value i v ~sync >>= fun wr_result ->
    begin
      if not (Value.is_master_set v )
      then Lwt.return ()
      else
        match (Value.update_from_value v) with
	      | Update.MasterSet (m,l) ->
	          begin
                let logit () =
                  let now = Int64.of_float (Unix.gettimeofday ()) in
                  let m_old_master = store # who_master () in
	              store # set_master_no_inc m now >>= fun _ ->
                  begin
                    let new_master =
                      begin
                        match m_old_master with
                          | Some(m_old,_) -> m <> m_old
                          | None -> true
                      end 
                    in
                    if (Int64.sub now !last_master_log_stmt >= 60L)  or new_master then
                      begin
                        last_master_log_stmt := now;
                        Lwt_log.info_f "%s is master"  m
                      end 
                    else 
                      Lwt.return ()
                  end
                in 
                logit ()
	          end
	      | _ -> Lwt.return()
    end 
      
  let reporting period backend () = 
    let fp = float period in
    let rec _inner () =
      Lwt_unix.sleep fp >>= fun () ->
      let stats = backend # get_statistics () in
      Lwt_log.info_f "stats: %s" (Statistics.Statistics.string_of stats) 
      >>= fun () ->
      let maxrss = Limits.get_maxrss() in
      let stat = Gc.stat () in
      let factor = float (Sys.word_size / 8) in
      let allocated = (stat.minor_words +.
			             stat.major_words -. stat.promoted_words) *. 
	    (factor /. 1024.0) 
      in
      Lwt_log.info_f "nallocated=%f KB; maxrss=%i KB" allocated maxrss
      >>= fun () ->
      backend # clear_most_statistics();
      _inner ()
    in
    _inner ()
end
  
let _main_2 
    (make_store: ?read_only:bool -> string -> Store.store Lwt.t) 
    make_tlog_coll make_config get_snapshot_name copy_store ~name 
    ~daemonize ~catchup_only =
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
            | None -> Lwt_log.info "Cluster not part of nursery."
            | Some (n_cluster_id, cfg) ->
              begin 
                let attempt_send success node_id = 
                  match success with
                    | true -> Lwt.return true
                    | false ->
                      begin
                        let (ips, port) = ClientCfg.get cfg node_id in
                        let ipss = Log_extra.list2s (fun s -> s) ips in
                        Lwt_log.debug_f "upload_cfg_to_keeper (%s,%i)" ipss port >>= fun () ->
                        Lwt.catch
                          (fun () ->
                            let ip0 = List.hd ips in
                            let address = Network.make_address ip0 port in
                            let upload connection = 
                              Remote_nodestream.make_remote_nodestream n_cluster_id connection >>= fun (client) ->
                              client # store_cluster_cfg cluster_id my_clicfg
                            in 
                            Lwt_io.with_connection address upload >>= fun () ->
                            Lwt_log.info_f "Successfully uploaded config to nursery node %s" node_id >>= fun () ->
                            Lwt.return true
                          ) 
                          (fun e ->
                            let exc_msg = Printexc.to_string e in
                            Lwt_log.warning_f "Attempt to upload config to %s failed (%s)" node_id exc_msg 
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
      let messaging  = _config_messaging me cfgs cookie me.is_laggy (float me.lease_period) cluster_cfg.max_buffer_size in
      Lwt_log.info_f "cfg = %s" (string_of me) >>= fun () ->
      Lwt_list.iter_s (Lwt_log.info_f "other: %s")
	    other_names >>= fun () ->
      Lwt_log.info_f "quorum_function gives %i for %i" 
	    (quorum_function n_nodes) n_nodes >>= fun () ->
      Lwt_log.info_f "DAEMONIZATION=%b" daemonize >>= fun () ->
      
      let build_startup_state () = 
	    begin
	      Node_cfg.Node_cfg.validate_dirs me >>= fun () ->
          let db_name = full_db_name me in
          let snapshot_name = get_snapshot_name() in
          let full_snapshot_path = Filename.concat me.tlog_dir snapshot_name in
          Lwt.catch 
            (fun () ->
              copy_store full_snapshot_path db_name false 
            ) 
            (function
              | Not_found -> Lwt.return ()
              | e -> raise e
            )
          >>= fun () ->
          make_store db_name >>= fun (store:Store.store) ->
          let store_i = store # consensus_i () in
          let s_i = 
            begin
		      match store_i with
		        | Some i -> i
		        | None -> Sn.start
            end
          in
	      Lwt.catch
	        (fun () -> make_tlog_coll me.tlog_dir me.use_compression ) 
	        (function 
              | Tlc2.TLCCorrupt (pos,tlog_i) ->
                let store_i = store # consensus_i () in
                Tlc2.get_last_tlog me.tlog_dir >>= fun (last_c, last_tlog) ->
                let tlog_i = 
                  begin
		            match tlog_i with
		              | i when i = Sn.start -> (Sn.mul (Sn.of_int !Tlogcommon.tlogEntriesPerFile) 
                                                  (Sn.of_int last_c) )
		              | _ -> tlog_i 
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
          let last_i = tlog_coll # get_last_i () in
          let ti_o = Some last_i in
          let current_i = last_i in (* ?? *)
	      Catchup.verify_n_catchup_store me.node_name 
	        (store, tlog_coll, ti_o) 
	        ~current_i master 
	      >>= fun (new_i, vo) ->
	      
	      let client_buffer =
	        let capacity = (Some 10) in
	        Lwt_buffer.create ~capacity () in
	      let client_push v = Lwt_buffer.add v client_buffer 
	      in
	      let node_buffer = messaging # get_buffer my_name in
	      let expect_reachable = messaging # expect_reachable in
	      let inject_buffer = Lwt_buffer.create_fixed_capacity 1 in
	      let inject_push v = Lwt_buffer.add v inject_buffer in
	      let read_only = master = ReadOnly in
	      let sb =
	        let test = Node_cfg.Node_cfg.test cluster_cfg in
	        new Sync_backend.sync_backend me 
              (client_push: (Update.t * (Store.update_result -> unit Lwt.t)) -> unit Lwt.t)
              inject_push
	          store (make_store, copy_store, full_snapshot_path) 
              tlog_coll lease_period
	          ~quorum_function n_nodes
	          ~expect_reachable
	          ~test
	          ~read_only
              ~max_value_size:cluster_cfg.max_value_size
	      in
	      let backend = (sb :> Backend.backend) in
	      
	      let service = _config_service me backend in
	      
	      let send, receive, run, register =
	        Multi_paxos.network_of_messaging messaging in
	      
	      let on_consensus = X.on_consensus store in
	      let on_witness (name:string) (i: Sn.t) = backend # witness name i in
	      let last_witnessed (name:string) = backend # last_witnessed name in
	      let on_accept = X.on_accept tlog_coll store in
	      
	      let get_last_value (i:Sn.t) = tlog_coll # get_last_value i in
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
	          on_accept 
              on_consensus
              on_witness
	          last_witnessed
	          (quorum_function: int -> int)
	          (master : master)
	          store 
              tlog_coll 
              others 
              lease_period 
              inject_event 
	          ~cluster_id
              false
	      in 
	      let reporting_period = me.reporting in
	      Lwt.return ((master,constants, buffers, new_i, vo, store), 
		              service, X.reporting reporting_period backend)
	    end
	      
      in
      
      let killswitch = Lwt_mutex.create () in
      Lwt_mutex.lock killswitch >>= fun () ->
      let unlock_killswitch () = Lwt_mutex.unlock killswitch in
      let listen_for_sigterm () = Lwt_mutex.lock killswitch in
      
      let start_backend (master, constants, buffers, new_i, vo, store) =
	    let to_run = 
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
        to_run constants buffers new_i vo
      in
      (*_maybe_daemonize daemonize me make_config >>= fun _ ->*)
      Lwt.catch
	    (fun () ->
          let _ = Lwt_unix.on_signal 15
            (fun i -> 
	          unlock_killswitch ()
	        )
          in
	      Lwt.pick [ 
	        messaging # run ();
	        begin
	          build_startup_state () >>= fun (start_state,
					                          service, 
					                          rapporting) -> 
              let (_,_,_,_,_,store) = start_state in
	          Lwt.pick[ start_backend start_state;
			            service ();
			            rapporting ();
                        (listen_for_sigterm () >>= fun () ->
			             Lwt_log.info "got sigterm" >>= fun () ->
			             Lwt_io.printlf "(stdout)got sigterm" >>= fun () ->
                         store # close () >>= fun () ->
                         Lwt_log.fatal_f 
                           ">>> Closing the store @ %S succeeded: everything seems OK <<<"
                           (store # get_location ())
                        )
			            ;
		              ]
	        end
	      ] >>= fun () ->
          Lwt_log.info "Completed shutdown"
        )
	    (fun exn -> 
	      Lwt_log.fatal ~exn "going down" >>= fun () ->
	      Lwt_log.fatal "after pick" >>= fun() ->
	      match dump_crash_log with
	        | None -> Lwt_log.info "Not dumping state"
	        | Some f -> f() )
    end
      
      
let main_t make_config name daemonize catchup_only=
  let make_store = Local_store.make_local_store in
  let make_tlog_coll = Tlc2.make_tlc2 in
  let get_snapshot_name = Tlc2.head_name in
  let copy_store = Local_store.copy_store in
  _main_2 make_store make_tlog_coll make_config get_snapshot_name copy_store ~name ~daemonize ~catchup_only
    
let test_t make_config name =
  let make_store = Mem_store.make_mem_store in
  let make_tlog_coll = Mem_tlogcollection.make_mem_tlog_collection in
  let get_snapshot_name = fun () -> "DUMMY" in
  let copy_store = Mem_store.copy_store in
  let daemonize = false 
  and catchup_only = false in
  _main_2 make_store make_tlog_coll make_config get_snapshot_name copy_store ~name ~daemonize ~catchup_only

