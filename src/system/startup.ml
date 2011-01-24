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

let _make_cfg name n master lease_period =
  {
    node_name = name;
    ip = "127.0.0.1";
    client_port =  4000 + n;
    messaging_port = 4010 + n;
    home = "#MEM#post_failure" ^ name;
    tlog_dir = "none";
    log_dir = "none";
    log_level = "DEBUG";
    lease_period = lease_period;
    forced_master = Some master;
  }


let post_failure () = 
  let lease_period = 10 in
  let master = "master" in
  let slave1 = "slave1" in
  let master_cfg = _make_cfg master 0 master lease_period in
  let slave1_cfg = _make_cfg slave1 1 master lease_period in
  let cfgs = [master_cfg;slave1_cfg] in
  let forced_master    = Some master in
  let quorum_function  n = (n/2) +1 in
  let make_store_master db_name = 
    Mem_store.make_mem_store db_name >>= fun store ->
    let now = Int64.of_float ( Unix.time() ) in
    store # set_master master now >>= fun () -> 
    Lwt.return store
  in
  let make_tlog_coll_master tlc_name = 
    Mem_tlogcollection.make_mem_tlog_collection tlc_name >>= fun tlc ->
    let u0 = Update.MasterSet(master,0L)  in
    let u1 = Update.Set("x","y") in
    tlc # log_update 0L u0 >>= fun _ ->
    tlc # log_update 1L u1 >>= fun _ ->
    Lwt.return tlc
  in
  let get_cfgs () = cfgs in

  let run_master () = 
    Node_main._main_2 
      make_store_master
      make_tlog_coll_master
      get_cfgs
      forced_master
      quorum_function 
      "master"
      ~daemonize:false
      lease_period
  in
  let make_store_slave1 db_name = 
    Mem_store.make_mem_store db_name >>= fun store ->
    let now = Int64.of_float ( Unix.time() ) in
    store # set_master master now >>= fun () ->
    Lwt.return store
  in
  let make_tlog_coll_slave1 name = 
    Mem_tlogcollection.make_mem_tlog_collection name >>= fun tlc ->
    let u0 = Update.MasterSet(master,0L) in
    let u1 = Update.Set("x","y") in
    tlc # log_update 0L u0 >>= fun _ ->
    tlc # log_update 1L u1 >>= fun _ ->
    return tlc
  in
  let run_slave1 () =
    Node_main._main_2
      make_store_slave1
      make_tlog_coll_slave1
      get_cfgs
      forced_master
      quorum_function
      "slave1"
      ~daemonize:false
      lease_period
  in
  let eventually_die () = 
    Lwt_unix.sleep 10.0 >>= fun () ->
    Lwt.fail (Failure "took too long")
  in
  Lwt_log.debug "start of scenario" >>= fun () ->
  Lwt.pick [run_master ();
	    run_slave1 ();
	    eventually_die ()] 
  >>= fun () ->
  Lwt_log.debug "end of scenario" >>= fun () ->
  Lwt.return ()
    




let setup () = Lwt.return ()
let teardown () = Lwt_log.debug "teardown"

let w f = Extra.lwt_bracket setup f teardown 

let suite = "startup" >:::[
  "post_failure" >:: w post_failure;
]
