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

let _make_cfg name n lease_period =
  {
    node_name = name;
    ip = "127.0.0.1";
    client_port =  4000 + n;
    messaging_port = 4010 + n;
    home = name;
    tlog_dir = name;
    log_dir = "none";
    log_level = "DEBUG";
    lease_period = lease_period;
    forced_master = None;
  }

let _make_tlog_coll tlcs updates tlc_name = 
  Mem_tlogcollection.make_mem_tlog_collection tlc_name >>= fun tlc ->
  let rec loop i = function
    | [] -> Lwt.return () 
    | u :: us -> 
      begin
	tlc # log_update i u >>= fun _ ->
	loop (Sn.succ i) us
      end
  in
  loop Sn.start updates >>= fun () ->
  Hashtbl.add tlcs tlc_name tlc;
  Lwt.return tlc

let _make_store stores now node_x db_name = 
  Mem_store.make_mem_store db_name >>= fun store ->
  store # set_master node_x now >>= fun () -> 
  Hashtbl.add stores db_name store;
  Lwt.return store

let _make_run 
    ?(forced_master=None) 
    ?(quorum_function= fun n -> (n/2)+1)
    ?(lease_period = 10)
    ~stores ~tlcs ~now ~updates ~get_cfgs node_x () = 
  Node_main._main_2 
    (_make_store stores now node_x)
    (_make_tlog_coll tlcs updates)
    get_cfgs
    forced_master
    quorum_function 
    node_x
    ~daemonize:false
    lease_period

let _dump_tlc ~tlcs node = 
  let tlc0 = Hashtbl.find tlcs node in
  let printer (i,u) = 
    Lwt_log.debug_f "%s:%s" (Sn.string_of i) (Update.string_of u) in
  Lwt_log.debug_f "--- %s ---" node >>= fun () ->
  tlc0 # iterate Sn.start 20L printer >>= fun () ->
  Lwt.return ()


let post_failure () = 
  let lease_period = 10 in
  let node0 = "was_master" in
  let node1 = "was_slave1" in
  let node2 = "was_slave2" in
  let node0_cfg = _make_cfg node0 0 lease_period in
  let node1_cfg = _make_cfg node1 1 lease_period in
  let node2_cfg = _make_cfg node2 2 lease_period in
  let cfgs = [node0_cfg;node1_cfg;node2_cfg] in
  let u0 = Update.MasterSet(node0,0L)  in
  let u1 = Update.Set("x","y") in
  let tlcs = Hashtbl.create 5 in
  let stores = Hashtbl.create 5 in
  let now = Int64.of_float( Unix.time() ) in
  let get_cfgs () = cfgs in
  let run_node0 = _make_run ~stores ~tlcs ~now ~get_cfgs ~updates:[u0;u1] node0 in
  let run_node1 = _make_run ~stores ~tlcs ~now ~get_cfgs ~updates:[u0;u1] node1 in
  let run_node2 = _make_run ~stores ~tlcs ~now ~get_cfgs ~updates:[u0]    node2 in
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
  let lease_period = 10 in
  let node0 = "slave0" in
  let node1 = "slave1" in
  let node2 = "was_master" in
  let node0_cfg = _make_cfg node0 0 lease_period in
  let node1_cfg = _make_cfg node1 1 lease_period in
  let node2_cfg = _make_cfg node2 2 lease_period in
  let cfgs = [node0_cfg;node1_cfg;node2_cfg] in
  let u0 = Update.MasterSet(node0,0L) in
  let u1 = Update.Set("xxx","xxx") in
  let tlcs = Hashtbl.create 5 in
  let stores = Hashtbl.create 5 in
  let now = Int64.of_float(Unix.time()) in
  let get_cfgs () = cfgs in 
  let run_node0 = _make_run ~stores ~tlcs ~now ~get_cfgs ~updates:[u0;u1] node0 in
  let run_node1 = _make_run ~stores ~tlcs ~now ~get_cfgs ~updates:[u0;u1] node1 in
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
