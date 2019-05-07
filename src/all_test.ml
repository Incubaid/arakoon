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
    Arakoon_inifiles_test.suite;
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
      Tlog_map_test.suite;
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
  ]

let system = "system" >::: [
    Single.force_master;
    Single.elect_master;
    Startup.suite;
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
    Client_cfg_test.suite;
    Node_cfg_test.suite;
    Arakoon_log_sink_test.suite;
    Arakoon_etcd_test.suite;
    Arakoon_client_config_test.suite;
    Arakoon_config_url_test.suite;
    Mp_msg_test.suite;
  ]
