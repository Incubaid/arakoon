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

let rec _split node_name cfgs =
  let rec loop me others = function
    | [] -> me, others
    | cfg :: rest ->
  if cfg.node_name = node_name then
    loop (Some cfg) others rest
  else
    loop me (cfg::others) rest
  in loop None [] cfgs

let _config_logging cfg =
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
  let file_name = log_dir ^ "/" ^ node_name ^ ".log" in
  if log_dir <> "none" then
    let logger = Lwt_log.file
      ~template:"$(date): $(level): $(message)"
      ~mode:`Append ~file_name ()
    in
    Lwt_log.default := logger;
    Lwt_log.Section.set_level Lwt_log.Section.main level

let _config_messaging me others =
  let mapping = List.map
    (fun cfg -> (cfg.node_name, (cfg.ip, cfg.messaging_port)))
    others
  in
  let messaging = new tcp_messaging (me.ip, me.messaging_port) in
    messaging # register_receivers mapping;
    (messaging :> Messaging.messaging)

open Mp_msg

let _check_tlogs collection tlog_dir =
  Lwt_log.info_f "checking tlog directory: %s" tlog_dir >>= fun () ->
  collection # validate_last_tlog () >>= fun (validity, lastI) ->
  begin
    match lastI with
      | None -> (Lwt_log.info_f "tlog is empty" )
      | Some i  -> Lwt_log.info_f "tlog @ %s" (Sn.string_of i)
  end >>= fun () ->
  Lwt.return lastI

let _maybe_daemonize daemonize cfg =
  if daemonize then
    begin
      Lwt_daemon.daemonize
	~syslog:false
	~stdin:`Close
	~stdout:`Dev_null
	~stderr:`Dev_null
	();
      _config_logging cfg
    end


let _config_service cfg backend=
  let port = cfg.client_port in
  Server.make_server_thread port (Client_protocol.protocol backend)

let _log_rotate cfg i =
  Lwt_log.warning_f "received USR (%i) going to close/reopen log file" i
  >>= fun () ->
  let logger = !Lwt_log.default in
  Lwt_log.close logger >>= fun () ->
  let () = _config_logging cfg in
  Lwt.return ()

let _main_2 make_store make_tlog_coll cfgs
    forced_master quorum_function name
    daemonize lease_expiry
    =

  let names = List.map (fun cfg -> cfg.node_name) cfgs in
  let n_names = List.length names in
  let other_names = List.filter (fun n -> n <> name) names in
  let splitted, others = _split name cfgs in
    match splitted with
      | None -> failwith (name ^ " is not known in config")
      | Some me ->
	  let () = _config_logging me in
	  let _ = Lwt_unix.on_signal 10
	    (fun i -> Lwt.ignore_result (_log_rotate me i))
	  in
	  Lwt_log.info "--- NODE STARTED ---" >>= fun () ->
	  let my_name = me.node_name in
	  let messaging  = _config_messaging me cfgs in
	  let never () = false in	  
	  let build_startup_state () = 
	    begin
	      Lwt_list.iter_s (fun n -> Lwt_log.info_f "other: %s" n)
		other_names >>= fun () ->
	      make_tlog_coll me.home >>= fun (tlog_coll:Tlogcollection.tlog_collection) ->
	      _check_tlogs tlog_coll me.home >>= fun tlogI ->
	      let db_name = me.home ^ "/" ^ my_name ^ ".db" in
	      make_store db_name >>= fun (store:Store.store) ->
	      let current_i = match tlogI with
		| None -> Sn.start 
		| Some i -> i
	      in
	      Catchup.verify_n_catchup_store me (store,tlog_coll,tlogI) current_i 
	      >>= fun new_i ->
	      
	      let client_buffer =
		let capacity = (Some 10) in
		Lwt_buffer.create ~capacity () in
	      let client_push v =
		Lwt_log.debug_f "pushing client event" >>= fun () ->
		Lwt_buffer.add v client_buffer >>= fun () ->
		Lwt_log.debug_f "pushing client event (done)"
	      in
	      let backend =
		new Sync_backend.sync_backend me client_push
		  store tlog_coll lease_expiry 
		  quorum_function n_names
	      in
	      
	      let service = _config_service me backend in
	      let send, receive, run, register =
		Multi_paxos.network_of_messaging messaging in
	      
	      let on_consensus (v,(n: Sn.t), (i: Sn.t) ) =
		Store.on_consensus store (v,n,i)
	      in
	      let on_witness (name:string) (i: Sn.t) = backend # witness name i 
	      in
	      let on_accept (v,n,i) =
		Lwt_log.debug_f "on_accept: %s %s %s" 
		  (Value.string_of v) (Sn.string_of n) (Sn.string_of i)
		>>= fun () ->
		let Value.V(update_string) = v in
		let u, _ = Update.from_buffer update_string 0 in
		tlog_coll # log_update i u >>= fun wr_result ->
		Lwt_log.debug_f "log_update %s=>%S" 
		  (Sn.string_of i) 
		  (Tlogwriter.string_of wr_result)
	      in
	      
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
	      let node_buffer = messaging # get_buffer my_name in
	      let inject_buffer = Lwt_buffer.create_fixed_capacity 1 in
	      let election_timeout_buffer = Lwt_buffer.create_fixed_capacity 1 in
	      let inject_event (e:Multi_paxos.paxos_event) =
		let buffer,name = 
		  match e with
		    | Multi_paxos.ElectionTimeout _ -> election_timeout_buffer, "election"
		    | _ -> inject_buffer, "inject"
		in
		Lwt_log.info_f "XXX injecting event into %s" name >>= fun () ->
		Lwt_buffer.add e buffer
	      in
	      let buffers = Multi_paxos_fsm.make_buffers
		(client_buffer,
		 node_buffer,
		 inject_buffer,
		 election_timeout_buffer)
	      in
	      let constants = 
		Multi_paxos.make my_name other_names send receive 
		  get_last_value 
		  on_accept on_consensus on_witness
		  quorum_function forced_master 
		  store tlog_coll others lease_expiry inject_event 

	      in Lwt.return ((forced_master,constants, buffers, new_i), service)
	    end
	      
	  in
	  let start_backend (forced_master, constants, buffers, new_i) =
	    let to_run = 
	      match forced_master with
		| Some master  -> if master = my_name 
		  then Multi_paxos_fsm.run_forced_master
		  else Multi_paxos_fsm.run_forced_slave 
		| None -> Multi_paxos_fsm.run_election
	    in to_run constants buffers new_i 
	  in
	  Lwt_log.info_f "cfg = %s" (string_of me) >>= fun () ->
	  let () = _maybe_daemonize daemonize me in
	  Lwt_log.info_f "DAEMONIZATION=%b" daemonize >>= fun () ->
	  Lwt.catch
	    (fun () ->
	      Lwt.pick [ 
		messaging # run ~stop:never ();
		begin
		  build_startup_state () >>= fun (start_state,service) -> 
		  join[ start_backend start_state;
			service ()];
		end
	      ])
	    (fun exn -> Lwt_log.fatal ~exn "going down")



let main_t make_config name daemonize =
  let cfgs, forced_master, quorum_function, lease_expiry, use_compression = 
    make_config () 
  in
  let make_store = Local_store.make_local_store in
  let make_tlog_coll = 
    if use_compression 
    then
      Tlc2.make_tlc2
    else
      Tlogcollection.make_file_tlog_collection 
  in
  _main_2
    make_store make_tlog_coll cfgs
    forced_master quorum_function name
    daemonize lease_expiry

let test_t make_config name =
  let cfgs, forced_master, quorum_function, lease_expiry, use_compression = 
    make_config () 
  in
  let make_store = Mem_store.make_mem_store in
  let make_tlog_coll = Mem_tlogcollection.make_mem_tlog_collection in
  _main_2 make_store make_tlog_coll cfgs
    forced_master quorum_function name
    false lease_expiry
