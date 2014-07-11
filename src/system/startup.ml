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

open OUnit
open Lwt

open Node_cfg.Node_cfg
open Update
open Master_type
open Tlogcommon

let section = Logger.Section.main

module LS = (val (Store.make_store_module (module Mem_store)))

let _make_log_cfg () =
  ("log_cfg",
   {
     client_protocol = None;
     paxos = None;
     tcp_messaging = None;
   })

let _make_batched_transaction_cfg () =
  ("batched_transaction_config",
   {
     max_entries = None;
     max_size = None;
   })

let _make_cfg name n lease_period =
  {
    node_name = name;
    ips = ["127.0.0.1"];
    client_port =  4000 + n;
    messaging_port = 4010 + n;
    home = name;
    tlog_dir = name;
    tlf_dir = name;
    head_dir = name;
    log_dir = "none";
    log_level = "DEBUG";
    log_config = Some "log_cfg";
    batched_transaction_config = Some "batched_transaction_config";
    lease_period = lease_period;
    master = Elected;
    is_laggy = false;
    is_learner = false;
    targets = [];
    use_compression = true;
    fsync = false;
    is_test = true;
    reporting = 300;
  }

let _make_tlog_coll tlcs values tlc_name tlf_dir head_dir use_compression _fsync node_id =
  Mem_tlogcollection.make_mem_tlog_collection tlc_name tlf_dir head_dir use_compression node_id >>= fun tlc ->
  let rec loop i = function
    | [] -> Lwt.return ()
    | v :: vs ->
      begin
	    tlc # log_value i v >>= fun () ->
	    loop (Sn.succ i) vs
      end
  in
  loop Sn.start values >>= fun () ->
  Hashtbl.add tlcs tlc_name tlc;
  Lwt.return tlc

let _make_run ~stores ~tlcs ~now ~values ~get_cfgs name () =
  let module S =
      struct
        include LS
        let make_store ?(read_only=false) (db_name:string) =
          let () = ignore read_only in
          LS.make_store db_name >>= fun store ->
          LS.with_transaction store (fun tx -> LS.set_master store tx name now) >>= fun () ->
          Hashtbl.add stores db_name store;
          Lwt.return store
      end in
  Node_main._main_2
    (module S)
    (_make_tlog_coll tlcs values)
    get_cfgs
    (fun () -> "DUMMY")
    ~name
    ~daemonize:false
    ~catchup_only:false
    >>= fun _ -> Lwt.return ()

let _dump_tlc ~tlcs node =
  let tlc0 = Hashtbl.find tlcs node in
  let printer entry =
    let i = Entry.i_of entry in
    let v = Entry.v_of entry in
    Logger.debug_f_ "%s:%s" (Sn.string_of i) (Value.value2s v)
  in
  Logger.debug_f_ "--- %s ---" node >>= fun () ->
  tlc0 # iterate Sn.start 20L printer >>= fun () ->
  Lwt.return ()


let post_failure () =
  let lease_period = 2 in
  let node0 = "was_master" in
  let node1 = "was_slave1" in
  let node2 = "was_slave2" in
  let node0_cfg = _make_cfg node0 0 lease_period in
  let node1_cfg = _make_cfg node1 1 lease_period in
  let node2_cfg = _make_cfg node2 2 lease_period in
  let cluster_cfg = {
    cfgs = [node0_cfg;node1_cfg;node2_cfg] ;
    log_cfgs = [_make_log_cfg ()];
    batched_transaction_cfgs = [_make_batched_transaction_cfg ()];
    _master = Elected;
    quorum_function = Quorum.quorum_function;
    _lease_period = 2;
    cluster_id = "ricky";
    plugins = [];
    nursery_cfg = None;
    overwrite_tlog_entries = None;
    max_value_size = Node_cfg.default_max_value_size;
    max_buffer_size = Node_cfg.default_max_buffer_size;
    client_buffer_capacity = Node_cfg.default_client_buffer_capacity;
  }
  in
  let get_cfgs () = cluster_cfg in
  let v0 = Value.create_master_value (node0,0.)  in
  let v1 = Value.create_client_value [Update.Set("x","y")] false in
  let tlcs = Hashtbl.create 5 in
  let stores = Hashtbl.create 5 in
  let now = Unix.gettimeofday () in

  let run_node0 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v0;v1] node0 in
  let run_node1 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v0;v1] node1 in
  let run_node2 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v0]    node2 in
  let eventually_stop () = Lwt_unix.sleep 10.0

  in
  Logger.debug_ "start of scenario" >>= fun () ->
  Lwt.pick [begin Lwt_unix.sleep 1.0 >>= fun () -> run_node0 () end;
	    begin Lwt_unix.sleep 5.0 >>= fun () -> run_node1 () end;
	    run_node2 ();
	    eventually_stop ()]
  >>= fun () ->
  Logger.debug_ "end of scenario" >>= fun () ->
  let check_store node =
    let db_name = (node ^ "/" ^ node ^".db") in
    let store0 = Hashtbl.find stores db_name in
    let key = "x" in
    LS.exists store0 key >>= fun b ->
    Logger.debug_f_ "%s: '%s' exists? -> %b" node key b >>= fun () ->
    OUnit.assert_bool (Printf.sprintf "value for '%s' should be in store" key) b;
    Lwt.return ()
  in
  Lwt_list.iter_s (_dump_tlc ~tlcs)   [node0;node1;node2]>>= fun () ->
  Lwt_list.iter_s check_store [node0;node1;node2]


let restart_slaves () =
  let lease_period = 2 in
  let node0 = "slave0" in
  let node1 = "slave1" in
  let node2 = "was_master" in
  let node0_cfg = _make_cfg node0 0 lease_period in
  let node1_cfg = _make_cfg node1 1 lease_period in
  let node2_cfg = _make_cfg node2 2 lease_period in
  let cluster_cfg =
    {cfgs = [node0_cfg;node1_cfg;node2_cfg];
     log_cfgs = [_make_log_cfg ()];
     batched_transaction_cfgs = [_make_batched_transaction_cfg ()];
     _master = Elected;
     quorum_function = Quorum.quorum_function;
     _lease_period = 2;
     cluster_id = "ricky";
     plugins = [];
     nursery_cfg = None;
     overwrite_tlog_entries = None;
     max_value_size = Node_cfg.default_max_value_size;
     max_buffer_size = Node_cfg.default_max_buffer_size;
     client_buffer_capacity = Node_cfg.default_client_buffer_capacity
    }
  in
  let get_cfgs () = cluster_cfg in
  let v0 = Value.create_master_value (node2, 0.) in
  let v1 = Value.create_client_value [Update.Set("xxx","xxx")] false in
  let tlcs = Hashtbl.create 5 in
  let stores = Hashtbl.create 5 in
  let now = Unix.gettimeofday () in

  let run_node0 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v0] node0 in
  let run_node1 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v0;v1] node1 in
  (* let run_node2 = _make_run ~stores ~tlcs ~now ~get_cfgs ~updates:[u0;u1] node2 in *)
  let eventually_stop() = Lwt_unix.sleep 10.0 in
  Logger.debug_ "start of scenario" >>= fun () ->
  Lwt.pick [run_node0 ();
	    run_node1 ();
	    (* run_node2 () *)
	   eventually_stop();
	   ]
  >>= fun () ->
  Logger.debug_ "end of scenario" >>= fun () ->
  let check_store node =
    let db_name = (node ^ "/" ^ node ^".db") in
    let store0 = Hashtbl.find stores db_name in
    let key = "xxx" in
    LS.exists store0 key >>= fun b ->
    Logger.debug_f_ "%s: '%s' exists? -> %b" node key b >>= fun () ->
    OUnit.assert_bool (Printf.sprintf "value for '%s' should be in store" key) b;
    Lwt.return ()
  in
  Lwt_list.iter_s (_dump_tlc ~tlcs)   [node0;node1]>>= fun () ->
  Lwt_list.iter_s check_store [node0;node1]


let ahead_master_loses_role () =
  let lease_period = 2 in
  let node0 = "slave0" in
  let node1 = "slave1" in
  let node2 = "was_master" in
  let node0_cfg = _make_cfg node0 0 lease_period in
  let node1_cfg = _make_cfg node1 1 lease_period in
  let node2_cfg = _make_cfg node2 2 lease_period in
  let cluster_cfg =
    {cfgs = [node0_cfg;node1_cfg;node2_cfg];
     log_cfgs = [_make_log_cfg ()];
     batched_transaction_cfgs = [_make_batched_transaction_cfg ()];
     _master = Elected;
     quorum_function = Quorum.quorum_function;
     _lease_period = 2;
     cluster_id = "ricky";
     plugins = [];
     nursery_cfg = None;
     overwrite_tlog_entries = None;
     max_value_size = Node_cfg.default_max_value_size;
     max_buffer_size = Node_cfg.default_max_buffer_size;
     client_buffer_capacity = Node_cfg.default_client_buffer_capacity;
    }
  in
  let get_cfgs () = cluster_cfg in
  let v0 = Value.create_master_value (node0, 0.) in
  let v1 = Value.create_client_value [Update.Set("xxx","xxx")] false in
  let v2 = Value.create_client_value [Update.Set("invalidkey", "shouldnotbepresent")] false in
  let tlcs = Hashtbl.create 5 in
  let stores = Hashtbl.create 5 in
  let now = Unix.gettimeofday () in

  let t_node0 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v0] node0 () in
  let t_node1 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v0;v1] node1 () in
  let run_previous_master = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v0;v1;v2] node2 in
  Logger.debug_ "start of scenario" >>= fun () ->
  Lwt.ignore_result t_node0;
  Lwt.ignore_result t_node1;
  (* sleep a bit so the previous 2 slaves can make progress *)
  Lwt_unix.sleep ((float lease_period) *. 1.5) >>= fun () ->
  let t_previous_master = run_previous_master () in
  Lwt.ignore_result t_previous_master;
  (* allow previous master to catch up with the others *)
  Lwt_unix.sleep 1. >>= fun () ->
  Logger.debug_ "end of scenario" >>= fun () ->
  List.iter (fun t -> Lwt.cancel t) [t_node0; t_node1; t_previous_master];
  let check_store node =
    let db_name = (node ^ "/" ^ node ^".db") in
    let store = Hashtbl.find stores db_name in
    let key = "invalidkey" in
    LS.exists store key >>= fun exists ->
    Logger.debug_f_ "%s: '%s' exists? -> %b" node key exists >>= fun () ->
    OUnit.assert_bool (Printf.sprintf "value for '%s' should not be in store" key) (not exists);
    Lwt.return ()
  in
  Lwt_list.iter_s (_dump_tlc ~tlcs)   [node0;node1;node2]>>= fun () ->
  Lwt_list.iter_s check_store [node0;node1;node2]


let interrupted_election () =
  let lease_period = 4 in
  let cluster_id = "ricky" in
  let wannabe_master = "wannabe_master" in
  let node2 = "node2" in
  let node3 = "node3" in
  let wannabe_master_cfg = _make_cfg wannabe_master 0 lease_period in
  let node2_cfg = _make_cfg node2 1 lease_period in
  let node3_cfg = _make_cfg node3 2 lease_period in
  let cluster_cfg =
    {cfgs = [wannabe_master_cfg;node2_cfg;node3_cfg];
     log_cfgs = [_make_log_cfg ()];
     batched_transaction_cfgs = [_make_batched_transaction_cfg ()];
     _master = Elected;
     quorum_function = Quorum.quorum_function;
     _lease_period = 2;
     cluster_id;
     plugins = [];
     nursery_cfg = None;
     overwrite_tlog_entries = None;
     max_value_size = Node_cfg.default_max_value_size;
     max_buffer_size = Node_cfg.default_max_buffer_size;
     client_buffer_capacity = Node_cfg.default_client_buffer_capacity;
    }
  in
  let get_cfgs () = cluster_cfg in
  let v0 = Value.create_master_value (wannabe_master, 0.) in
  let tlcs = Hashtbl.create 5 in
  let stores = Hashtbl.create 5 in
  let now = Unix.gettimeofday () in
  let t_node2 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0] node2 () in
  let t_node3 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0] node3 () in
  Lwt.ignore_result t_node2;
  Lwt.ignore_result t_node3;
  (* 2 phases: - wait until wannabe_master is the master
               - wait until another node becomes the master
     and a timeout on all this
   *)
  let get_master_from cfg =
    Client_main.with_client
      cfg cluster_id
      (fun client -> client # who_master ())
  in
  let rec phase1 () =
    Lwt.catch
      (fun () ->
       get_master_from node2_cfg >>=
         function
         | Some m ->
            if m = wannabe_master
            then Lwt.return true
            else failwith (Printf.sprintf "%s became master instead of %s" m wannabe_master)
         | None ->
            begin
              get_master_from node3_cfg >>=
                function
                | None ->
                   Lwt.return false
                | Some m ->
                   if m = wannabe_master
                   then Lwt.return true
                   else failwith (Printf.sprintf "%s became master instead of %s" m wannabe_master)
            end)
      (function
        | Unix.Unix_error(Unix.ECONNREFUSED, "connect", _) ->
           Lwt.return false
        | exn -> Lwt.fail exn) >>= fun continue ->
    if continue
    then Lwt.return ()
    else Lwt_unix.sleep 1. >>= fun () ->
         phase1 ()
  in

  let rec phase2 () =
    Lwt.catch
      (fun () ->
       Client_main.find_master cluster_cfg >>= fun m ->
       if m <> wannabe_master
       then Lwt.return true
       else Lwt.return false)
      (function
        | Failure "No Master" ->
           Lwt.return false
        | exn -> Lwt.fail exn) >>= fun continue ->
    if continue
    then
      Lwt.return ()
    else
      Lwt_unix.sleep 1. >>= fun () ->
      phase2 () in

  Lwt.pick
    [(let t0 = Unix.gettimeofday () in
      phase1 () >>= fun () ->
      phase2 () >>= fun () ->
      let t1 = Unix.gettimeofday () in
      let delta = t1 -. t0 in
      if (delta > (float lease_period))
      then Lwt.return ()
      else Lwt.fail (Failure (Printf.sprintf "test only took while %f seconds while expected duration is at least %i seconds" delta lease_period)));
     (Lwt_unix.timeout 60. >>= fun () -> failwith "test did not finish quick enough")]
  >>= fun () ->

  List.iter (fun t -> Lwt.cancel t) [t_node2; t_node3;];

  Lwt_list.iter_s (_dump_tlc ~tlcs)   [node2; node3]


let setup () = Lwt.return ()
let teardown () = Logger.debug_ "teardown"

let w f = Extra.lwt_bracket setup f teardown

let suite = "startup" >:::[
  "post_failure" >:: w post_failure;
  "restart_slaves" >:: w restart_slaves;
  "ahead_master_loses_role" >:: w ahead_master_loses_role;
  "interrupted_election" >:: w interrupted_election;
]
