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

(*
let test_t make_config name =
  let cfgs, forced_master, quorum_function, 
    lease_expiry, 
    use_compression = 
    make_config () 
  in
  let make_store = Mem_store.make_mem_store in
  let make_tlog_coll = Mem_tlogcollection.make_mem_tlog_collection in
  let get_cfgs () = cfgs in
  _main_2 make_store make_tlog_coll get_cfgs
    forced_master quorum_function name
    false lease_expiry
*)

open Node_cfg.Node_cfg
open Update

let _make_cfg name n lease_period =
  {
    node_name = name;
    ip = "127.0.0.1";
    client_port =  4000 + n;
    messaging_port = 4010 + n;
    home = "#MEM#post_failure" ^ name;
    tlog_dir = name;
    log_dir = "none";
    log_level = "DEBUG";
    lease_period = lease_period;
    forced_master = None;
  }


let post_failure () = 
  let lease_period = 10 in
  let node0 = "was_master" in
  let node1 = "was_slave1" in
  let node2 = "was_slave2" in
  let node0_cfg = _make_cfg node0 0 lease_period in
  let node1_cfg = _make_cfg node1 1 lease_period in
  let node2_cfg = _make_cfg node2 2 lease_period in
  let cfgs = [node0_cfg;node1_cfg;node2_cfg] in
  let forced_master    = None in
  let quorum_function  n = (n/2) +1 in
  let u0 = Update.MasterSet(node0,0L)  in
  let u1 = Update.Set("x","y") in
  let tlcs = Hashtbl.create 5 in
  let make_store_node0 db_name = 
    Mem_store.make_mem_store db_name >>= fun store ->
    store # set_master node0 >>= fun () -> 
    Lwt.return store
  in
  let make_tlog_coll_node0 tlc_name = 
    Mem_tlogcollection.make_mem_tlog_collection tlc_name >>= fun tlc ->
    tlc # log_update 0L u0 >>= fun _ ->
    tlc # log_update 1L u1 >>= fun _ ->
    Hashtbl.add tlcs tlc_name tlc;
    Lwt.return tlc
  in
  let get_cfgs () = cfgs in

  let run_node0 () = 
    Node_main._main_2 
      make_store_node0
      make_tlog_coll_node0
      get_cfgs
      forced_master
      quorum_function 
      node0
      ~daemonize:false
      lease_period
  in
  let make_store_node1 db_name = 
    Mem_store.make_mem_store db_name >>= fun store ->
    store # set_master node0 >>= fun () ->
    Lwt.return store
  in
  let make_tlog_coll_node1 name = 
    Mem_tlogcollection.make_mem_tlog_collection name >>= fun tlc ->
    tlc # log_update 0L u0 >>= fun _ ->
    tlc # log_update 1L u1 >>= fun _ ->
    Hashtbl.add tlcs name tlc;
    return tlc
  in
  let run_node1 () =
    Node_main._main_2
      make_store_node1
      make_tlog_coll_node1
      get_cfgs
      forced_master
      quorum_function
      node1
      ~daemonize:false
      lease_period
  in
  let make_store_node2 db_name = 
    Mem_store.make_mem_store db_name >>= fun store ->
    store # set_master node0 >>= fun () ->
    Lwt.return store
  in
  let make_tlog_coll_node2 name = 
    Mem_tlogcollection.make_mem_tlog_collection name >>= fun tlc ->
    tlc # log_update 0L u0 >>= fun _ ->
    Hashtbl.add tlcs name tlc;
    return tlc
  in
  let run_node2 () = 
    Node_main._main_2
      make_store_node2
      make_tlog_coll_node2
      get_cfgs
      forced_master
      quorum_function
      node2
      ~daemonize:false
      lease_period
  in
  let eventually_stop () = Lwt_unix.sleep 10.0 

  in
  Lwt_log.debug "start of scenario" >>= fun () ->
  Lwt.pick [run_node0 ();
	    begin Lwt_unix.sleep 1.0 >>= fun () -> run_node1 () end;
	    run_node2 ();
	    eventually_stop ()] 
  >>= fun () ->
  Lwt_log.debug "end of scenario" >>= fun () ->
  let dump node = 
    let tlc0 = Hashtbl.find tlcs node in
    let printer (i,u) = 
      Lwt_io.printlf "%s:%s" (Sn.string_of i) (Update.string_of u) in
    Lwt_io.printlf "--- %s ---" node >>= fun () ->
    tlc0 # iterate Sn.start 20L printer >>= fun () ->
    Lwt.return ()
  in
  Lwt_list.iter_s dump [node0;node1;node2]
    


let setup () = Lwt.return ()
let teardown () = Lwt_log.debug "teardown"

let w f = Extra.lwt_bracket setup f teardown 

let suite = "startup" >:::[
  "post_failure" >:: w post_failure;
]
