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
    tlx_dir = name;
    head_dir = name;
    log_dir = "none";
    log_level = "DEBUG";
    log_config = Some "log_cfg";
    batched_transaction_config = Some "batched_transaction_config";
    lease_period = lease_period;
    master = Elected;
    is_laggy = false;
    is_learner = false;
    is_witness = false;
    targets = [];
    compressor = Compression.Snappy;
    fsync = false;
    _fsync_tlog_dir = false;
    is_test = true;
    reporting = 300;
    node_tls = None;
    collapse_slowdown = None;
    head_copy_throttling = 0.0;
  }

let _make_tlog_coll ~compressor tlcs values tlc_name tlf_dir head_dir
                    ~fsync node_id ~fsync_tlog_dir =
  let () = ignore compressor in
  Mem_tlogcollection.make_mem_tlog_collection tlc_name tlf_dir head_dir ~fsync node_id ~fsync_tlog_dir >>= fun tlc ->
  let rec loop i = function
    | [] -> Lwt.return ()
    | (_, c) :: vs ->
      begin
        let v = Value.create_value tlc i c in
        tlc # log_value i v >>= fun () ->
        loop (Sn.succ i) vs
      end
  in
  loop Sn.start values >>= fun () ->
  Hashtbl.add tlcs tlc_name tlc;
  Lwt.return tlc

let stop = ref (ref false)
let node_ts = ref []

let _make_run_rc ~stores ~tlcs ~now ~values ~get_cfgs name () =
  let module S =
  struct
    include LS
    let make_store
        ~lcnum ~ncnum
        ?(read_only=false) (db_name:string) =
      LS.make_store ~lcnum ~ncnum ~read_only db_name >>= fun store ->
      LS.with_transaction store (fun tx -> LS.set_master store tx name now) >>= fun () ->
      Hashtbl.add stores db_name store;
      Lwt.return store
  end in
  let t = Node_main._main_2
    (module S)
    (_make_tlog_coll tlcs values)
    get_cfgs
    (fun () -> "DUMMY")
    ~name
    ~daemonize:false
    ~catchup_only:false
    ~stop:!stop in
  node_ts := (Lwt.map ignore t) :: !node_ts;
  t

let _make_run ~stores ~tlcs ~now ~values ~get_cfgs name () =
  _make_run_rc ~stores ~tlcs ~now ~values ~get_cfgs name () >>= fun _ ->
  Lwt.return ()

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
  let node0 = "post_failure_was_master" in
  let node1 = "post_failure_was_slave1" in
  let node2 = "post_failure_was_slave2" in
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
    lcnum = 8192;
    ncnum = 4096;
    tls = None;
  }
  in
  let get_cfgs () = cluster_cfg in
  let v0 = Value.create_master_value_nocheck ~lease_start:0. node0 in
  let v1 = Value.create_client_value_nocheck [Update.Set("x","y")] false in
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
    let b = LS.exists store0 key in
    Logger.debug_f_ "%s: '%s' exists? -> %b" node key b >>= fun () ->
    OUnit.assert_bool (Printf.sprintf "value for '%s' should be in store" key) b;
    Lwt.return ()
  in
  Lwt_list.iter_s (_dump_tlc ~tlcs)   [node0;node1;node2]>>= fun () ->
  Lwt_list.iter_s check_store [node0;node1;node2]


let restart_slaves () =
  let lease_period = 2 in
  let node0 = "restart_slaves_slave0" in
  let node1 = "restart_slaves_slave1" in
  let node2 = "restart_slaves_was_master" in
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
     lcnum = Node_cfg.default_lcnum;
     ncnum = Node_cfg.default_ncnum;
     tls = None;
    }
  in
  let get_cfgs () = cluster_cfg in
  let v0 = Value.create_master_value_nocheck ~lease_start:0. node2 in
  let v1 = Value.create_client_value_nocheck [Update.Set("xxx","xxx")] false in
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
    let b = LS.exists store0 key in
    Logger.debug_f_ "%s: '%s' exists? -> %b" node key b >>= fun () ->
    OUnit.assert_bool (Printf.sprintf "value for '%s' should be in store" key) b;
    Lwt.return ()
  in
  Lwt_list.iter_s (_dump_tlc ~tlcs)   [node0;node1]>>= fun () ->
  Lwt_list.iter_s check_store [node0;node1]


let ahead_master_loses_role () =
  let lease_period = 2 in
  let node0 = "ahead_master_loses_role_slave0" in
  let node1 = "ahead_master_loses_role_slave1" in
  let node2 = "ahead_master_loses_role_was_master" in
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
     lcnum = 8192;
     ncnum = 4096;
     tls = None;
    }
  in
  let get_cfgs () = cluster_cfg in
  let v0 = Value.create_master_value_nocheck ~lease_start:0. node0 in
  let v1 = Value.create_client_value_nocheck [Update.Set("xxx","xxx")] false in
  let v2 = Value.create_client_value_nocheck [Update.Set("invalidkey", "shouldnotbepresent")] false in
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
    let exists = LS.exists store key in
    Logger.debug_f_ "%s: '%s' exists? -> %b" node key exists >>= fun () ->
    OUnit.assert_bool (Printf.sprintf "value for '%s' should not be in store" key) (not exists);
    Lwt.return ()
  in
  Lwt_list.iter_s (_dump_tlc ~tlcs)   [node0;node1;node2]>>= fun () ->
  Lwt_list.iter_s check_store [node0;node1;node2]


let interrupted_election () =
  let lease_period = 4 in
  let cluster_id = "ricky" in
  let wannabe_master = "interrupted_election_wannabe_master" in
  let node2 = "interrupted_election_node2" in
  let node3 = "interrupted_election_node3" in
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
     lcnum = 8192;
     ncnum = 4096;
     tls = None;
    }
  in
  let get_cfgs () = cluster_cfg in
  let v0 = Value.create_master_value_nocheck ~lease_start:0. wannabe_master in
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
      cfg ~tls:None cluster_id
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
       Client_main.find_master ~tls:None cluster_cfg >>= fun m ->
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


let disk_failure () =
  let lease_period = 2 in
  let node0 = "disk_failure_slave0" in
  let node1 = "disk_failure_slave1" in
  let node2 = "disk_failure_was_master" in
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
     lcnum = 8192;
     ncnum = 4096;
     tls = None;
    }
  in
  let get_cfgs () = cluster_cfg in
  let v0 = Value.create_master_value_nocheck ~lease_start:0. node2 in
  let v1 = Value.create_client_value_nocheck [Update.Set("a","a")] false in
  let v2 = Value.create_client_value_nocheck [Update.Set("b","b")] false in
  let v3 = Value.create_client_value_nocheck [Update.Set("c","c")] false in
  let tlcs = Hashtbl.create 5 in
  let stores = Hashtbl.create 5 in
  let now = Unix.gettimeofday () in

  let t_node0 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v1] node0 () in
  let t_node1 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v1] node1 () in
  let run_previous_master = _make_run_rc ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v1;v2;v3] node2 in
  Logger.debug_ "start of scenario" >>= fun () ->
  Lwt.ignore_result t_node0;
  Lwt.ignore_result t_node1;
  (* sleep a bit so the previous 2 slaves can make progress *)
  Lwt_unix.sleep ((float lease_period) *. 1.5) >>= fun () ->
  let t_previous_master = run_previous_master () >>= function
    | 51 -> Lwt.return ()
    | 0 -> Lwt.return (OUnit.assert_bool "the node should fail" false)
    | rc ->
      let msg = Printf.sprintf "it threw the wrong exception: %i" rc in
      Lwt.return (OUnit.assert_bool msg false) in
  Lwt.ignore_result t_previous_master;
  (* allow previous master to catch up with the others *)
  Lwt_unix.sleep 1. >>= fun () ->
  Logger.debug_ "end of scenario" >>= fun () ->
  List.iter (fun t -> Lwt.cancel t) [t_node0; t_node1; t_previous_master];
  Lwt_list.iter_s (_dump_tlc ~tlcs) [node0; node1; node2]


let setup () = Lwt.return ()
let teardown () =
  !stop := true;
  stop := ref false;
  Lwt.join !node_ts >>= fun () ->
  node_ts := [];
  Logger.debug_ "teardown"

let w f = Extra.lwt_bracket setup f teardown

let suite = "startup" >:::[
    "post_failure" >:: w post_failure;
    "restart_slaves" >:: w restart_slaves;
    "ahead_master_loses_role" >:: w ahead_master_loses_role;
    "interrupted_election" >:: w interrupted_election;
    "disk_failure" >:: w disk_failure;
  ]
