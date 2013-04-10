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

let _make_log_cfg () =
  ("log_cfg",
   {
     client_protocol = "debug";
     paxos = "debug";
     tcp_messaging = "debug";
   })

let _make_cfg name n lease_period =
  {
    node_name = name;
    ips = ["127.0.0.1"];
    client_port =  4000 + n;
    messaging_port = 4010 + n;
    home = name;
    tlog_dir = name;
    log_dir = "none";
    log_level = "DEBUG";
    log_config = Some "log_cfg";
    lease_period = lease_period;
    master = Elected;
    is_laggy = false;
    is_learner = false;
    targets = [];
    use_compression = true;
    is_test = true;
    reporting = 300;
  }

let _make_tlog_coll tlcs values tlc_name use_compression node_id = 
  Mem_tlogcollection.make_mem_tlog_collection tlc_name use_compression node_id >>= fun tlc ->
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

let _make_store ?(read_only=false) stores now node_name (db_name:string) =
  Mem_store.make_mem_store db_name >>= fun store ->
  store # set_master node_name now >>= fun () -> 
  Hashtbl.add stores db_name store;
  Lwt.return store

let _make_run ~stores ~tlcs ~now ~values ~get_cfgs name () = 
  let sm ?(read_only=false) db_name = _make_store stores now name db_name in
  Node_main._main_2 
    sm
    (_make_tlog_coll tlcs values)
    get_cfgs
    (fun () -> "DUMMY")
    (fun s d o -> Lwt.return () )
    ~name
    ~daemonize:false
    ~catchup_only:false
    >>= fun _ -> Lwt.return ()

let _dump_tlc ~tlcs node = 
  let tlc0 = Hashtbl.find tlcs node in
  let printer entry = 
    let i = Entry.i_of entry in
    let v = Entry.v_of entry in
    Lwt_log.debug_f "%s:%s" (Sn.string_of i) (Value.value2s v) 
  in
  Lwt_log.debug_f "--- %s ---" node >>= fun () ->
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
  let v0 = Value.create_master_value (node0,0L)  in
  let v1 = Value.create_client_value [Update.Set("x","y")] false in
  let tlcs = Hashtbl.create 5 in
  let stores = Hashtbl.create 5 in
  let now = Int64.of_float( Unix.time() ) in

  let run_node0 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v1] node0 in
  let run_node1 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v1] node1 in
  let run_node2 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0]    node2 in
  let eventually_stop () = Lwt_unix.sleep 10.0 

  in
  Lwt_log.debug "start of scenario" >>= fun () ->
  Lwt.pick [run_node0 ();
	    begin Lwt_unix.sleep 5.0 >>= fun () -> run_node1 () end;
	    run_node2 ();
	    eventually_stop ()] 
  >>= fun () ->
  Lwt_log.debug "end of scenario" >>= fun () ->
  let check_store node = 
    let db_name = (node ^ "/" ^ node ^".db") in
    let store0 = Hashtbl.find stores db_name in
    let key = "x" in
    store0 # exists key >>= fun b ->
    Lwt_log.debug_f "%s: '%s' exists? -> %b" node key b >>= fun () ->
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
  let v0 = Value.create_master_value (node0, 0L) in
  let v1 = Value.create_client_value [Update.Set("xxx","xxx")] false in
  let tlcs = Hashtbl.create 5 in
  let stores = Hashtbl.create 5 in
  let now = Int64.of_float(Unix.time()) in
  
  let run_node0 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v1] node0 in
  let run_node1 = _make_run ~stores ~tlcs ~now ~get_cfgs ~values:[v0;v1] node1 in
  (* let run_node2 = _make_run ~stores ~tlcs ~now ~get_cfgs ~updates:[u0;u1] node2 in *)
  let eventually_stop() = Lwt_unix.sleep 10.0 in
  Lwt_log.debug "start of scenario" >>= fun () ->
  Lwt.pick [run_node0 ();
	    run_node1 ();
	    (* run_node2 () *)
	   eventually_stop();
	   ]
  >>= fun () ->
  Lwt_log.debug "end of scenario" >>= fun () ->
  let check_store node = 
    let db_name = (node ^ "/" ^ node ^".db") in
    let store0 = Hashtbl.find stores db_name in
    let key = "xxx" in
    store0 # exists key >>= fun b ->
    Lwt_log.debug_f "%s: '%s' exists? -> %b" node key b >>= fun () ->
    OUnit.assert_bool (Printf.sprintf "value for '%s' should be in store" key) b;
    Lwt.return ()
  in
  Lwt_list.iter_s (_dump_tlc ~tlcs)   [node0;node1]>>= fun () ->
  Lwt_list.iter_s check_store [node0;node1]
    

let setup () = Lwt.return ()
let teardown () = Lwt_log.debug "teardown"

let w f = Extra.lwt_bracket setup f teardown 

let suite = "startup" >:::[
  "post_failure" >:: w post_failure;
  "restart_slaves" >:: w restart_slaves;
]
