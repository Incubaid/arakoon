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

let configure_logging () =
  let logger = Lwt_log.channel
                 ~close_mode:`Keep
                 ~channel:Lwt_io.stderr
                 ~template:"$(date): $(level): $(message)"
                 ()
  in
  Lwt_log.default := logger;
  Logger.Section.set_level Logger.Section.main Logger.Debug

let tools_tests = "tools" >::: [
    Server_test.suite;
    Backoff_test.suite;
    Cllio_test.suite;
  ]

let client_tests = "client" >::: [
    Arakoon_remote_client_test.suite;
    Remote_nodestream_test.suite;
    Statistics_test.suite;
  ]

let tcp_messaging_tests = "messaging" >::: [Tcp_messaging_test.suite]

let paxos_tests = "paxos" >::: [Multi_paxos_test.suite;]

let update_tests = "updates" >::: [Update_test.suite]

let tlog_tests = "tlogs" >::: [
    Tlogcollection_test.suite_mem;
    Tlc2_test.suite;
    Tlogreader2_test.suite;
  ]
let small_catchup = "small_catchup" >:::[Catchup_test.suite;]
let store = "store" >:::[Store_test.suite;]
let compression = "compression" >::: [Compression_test.suite;]
let collapser   = "collapser" >::: [Collapser_test.suite]
let crc32c_tests = "crc32c" >::: [Crc32c_test.suite]

let nursery = "nursery" >::: [
    Routing_test.suite;
    Client_cfg_test.suite;
    Node_cfg_test.suite;
  ]

let system = "system" >::: [
    Single.force_master;
    Single.elect_master;(*
  Startup.suite;*)
    Drop_master.suite;
  ]

let lwt_socket_tests = "lwt" >::: [Lwt_socket_test.suite]

let suite = "universe" >::: [
    lwt_socket_tests;
    crc32c_tests;
    tools_tests;
    client_tests;
    tcp_messaging_tests;
    paxos_tests;
    update_tests;
    tlog_tests;
    small_catchup;
    compression;
    collapser;
    system;
    nursery;
    store;
  ]
