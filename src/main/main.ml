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

open OUnit_XML
open OUnit
open Node_cfg

type local_action =
  | ShowUsage
  | RunAllTests
  | RunAllTestsXML
  | RunSomeTests
  | ListTests
  | SystemTests
  | ShowVersion
  | DumpTlog
  | StripTlog
  | ReplayTlogs
  | DumpStore
  | MakeTlog
  | MarkTlog
  | CloseTlog
  | TruncateTlog
  | CompressTlog
  | UncompressTlog
  | BENCHMARK
  | LOAD
  | SET
  | GET
  | DELETE
  | DELETE_PREFIX
  | WHO_MASTER
  | EXPECT_PROGRESS_POSSIBLE
  | STATISTICS
  | PREFIX
  | Collapse_remote
  | Backup_db
  | Optimize_db
  | Defrag_db
  | NumberOfValues
  | InitNursery
  | MigrateNurseryRange
  | DeleteNurseryCluster
  | PING
  | InjectAsHead
  | NODE_VERSION
  | Drop_master
  | NODE_STATE
  | RANGE_ENTRIES
  | REV_RANGE_ENTRIES

type server_action =
  | Node
  | TestNode

type action =
  | LocalAction of local_action
  | ServerAction of server_action;;

let do_path p = Printf.printf "%s\n" (OUnit.string_of_path p)

let list_tests() =
  List.iter do_path (OUnit.test_case_paths All_test.suite)

let show_version ()=
  let open Arakoon_version in
  let () =
    Printf.printf "version: %i.%i.%i\n" major minor patch;
    Printf.printf "git_revision: %S\n" git_revision;
    Printf.printf "compiled: %S\n" compile_time;
    Printf.printf "machine: %S\n" machine;
    Printf.printf "compiler_version : %S\n" compiler_version;
    Printf.printf "tlogEntriesPerFile: %i\n" !Tlogcommon.tlogEntriesPerFile;
    Printf.printf "dependencies:\n%s\n" dependencies in
  ()

let interpret_test_results =
  List.fold_left
    (fun acc r ->
     match r with
     | RSuccess _ -> acc
     | RFailure _
     | RError _
     | RSkip _
     | RTodo _ -> 1)
    0

let run_all_tests () =
  All_test.configure_logging();
  let tests = All_test.suite in
  let results = run_test_tt tests in
  interpret_test_results results

let run_all_tests_xml filename =
  All_test.configure_logging();
  let tests = All_test.suite in
  let result = { result_list = [];
                 total_time = 0.0;
                 total_success = 0;
                 total_failures = 0;
                 total_errors = 0;
                 total_disabled = 0;
                 tmp_start = 0.0;
                 tmp_result = RSuccess []
               } in
  let results = perform_timed_tests result tests in
  let () = print_xml result filename in
  interpret_test_results results

let run_system_tests () =
  All_test.configure_logging();
  let tests = System_tests.suite in
  let results = OUnit.run_test_tt tests in
  interpret_test_results results


let run_some_tests repeat_count filter =
  All_test.configure_logging();
  Printf.printf "running tests matching '%s'\n" filter;
  let parts = Str.split_delim (Str.regexp "\\.") filter in
  let show_count num =
    if repeat_count > 1 then Printf.printf "-- iteration %d\n" (repeat_count - num + 1)
  in
  let rec do_n_times f results = function
    | 0 -> results
    | n ->
      let () = show_count n in
      let results' = f () in
      do_n_times f (List.append results' results) (n-1)
  in
  match OUnit.test_filter parts All_test.suite  with
    | Some test ->
      begin
        List.iter do_path (OUnit.test_case_paths test);
        let results = do_n_times (fun () -> OUnit.run_test_tt_main test) [] repeat_count in
        interpret_test_results results
      end

    | None -> failwith (Printf.sprintf "no test matches '%s'" filter);;


let main () =
  Ssl_threads.init ();
  Ssl.init ~thread_safe:true ();

  let _ = Bz2.version in
  let () = Sys.set_signal Sys.sigpipe Sys.Signal_ignore in
  let () = Random.self_init () in
  let () = Client_log.enable_lwt_logging_for_client_lib_code () in
  let usage_buffer = Buffer.create 1024 in
  let app = Buffer.add_string usage_buffer in
  let usage =
    let bin = Sys.argv.(0) in
    app ("usage: " ^ bin ^ " --<command>\n");
    app ("\nIf you're clueless, try " ^ bin ^ " --help\n");
    Buffer.contents usage_buffer
  in
  let action = ref (LocalAction ShowUsage)
  and filename = ref "toy.db"
  and xml_filename = ref "output.xml"
  and filter = ref ""
  and node_id = ref ""
  and key = ref ""
  and value = ref ""
  and ip = ref "127.0.0.1"
  and port = ref 4000
  and cluster_id = ref "<none>"
  and value_size = ref 10
  and key_size = ref 10
  and tx_size = ref 100
  and max_n = ref (1000 * 1000)
  and daemonize = ref false
  and test_repeat_count = ref 1
  and counter = ref 0
  and n_tlogs = ref 1
  and n_clients = ref 1
  and catchup_only = ref false
  and dump_values = ref false
  and max_results = ref 1000
  and location = ref ""
  and left_cluster = ref ""
  and separator = ref ""
  and right_cluster = ref ""
  and tlog_dir = ref ""
  and tlf_dir = ref ""
  and end_i = ref None
  and tls_ca_cert = ref ""
  and tls_cert = ref ""
  and tls_key = ref ""
  and tls_version = ref "1.0"
  and force = ref false
  and in_place = ref false
  and archive_type = ref ".tlf"
  and left = ref None
  and linc = ref true
  and right = ref None
  and rinc = ref false
  and scenario = ref (String.concat ", " Benchmark.default_scenario)
  in
  let set_action a = Arg.Unit (fun () -> action := a) in
  let set_laction a = set_action (LocalAction a) in
  let actions = [
    ("--node", Arg.Tuple [set_action (ServerAction Node);
                          Arg.Set_string node_id;
                         ],
     "runs a node");
    (* tempory test node, TODO: remove later *)
    ("--test-node", Arg.Tuple [set_action (ServerAction TestNode);
                               Arg.Set_string node_id;
                              ],
     "runs a node");
    ("--list-tests", set_laction ListTests, "lists all possible tests");
    ("--run-all-tests", set_laction RunAllTests, "runs all tests");

    ("--run-all-tests-xml", Arg.Tuple [set_laction RunAllTestsXML;
                                       Arg.Set_string xml_filename],
     "<filename> : runs all tests with XML output to file");

    ("--run-some-tests", Arg.Tuple [set_laction RunSomeTests;
                                    Arg.Set_string filter],
     "run tests matching filter");
    ("--truncate-tlog", Arg.Tuple[ set_laction TruncateTlog;
                                   Arg.Set_string filename],
     "<filename> : truncate a tlog after the last valid entry");
    ("--dump-tlog", Arg.Tuple[ set_laction DumpTlog;
                               Arg.Set_string filename],
     "<filename> : dump a tlog file in readable format");
    ("--strip-tlog", Arg.Tuple[ set_laction StripTlog;
                                Arg.Set_string filename],
     "<filename> : remove the marker of a tlog (development)");
    ("--mark-tlog", Arg.Tuple[ set_laction MarkTlog;
                               Arg.Set_string filename;
                               Arg.Set_string key;
                             ],
     "<filename> <key>: add a marker to a tlog");
    ("--unsafe-close-tlog", Arg.Tuple[ set_laction CloseTlog;
                               Arg.Set_string filename;
                               Arg.Set_string node_id;
                             ],
     "<filename> <node_name>: marks the tlog with 'closed:node_name'. Note when a tlog marker is missing, the node Tokyo Cabinet database could be silently corrupted, which is not fixed by this command.");
    ("--replay-tlogs", Arg.Tuple[ set_laction ReplayTlogs;
                                  Arg.Set_string tlog_dir;
                                  Arg.Set_string tlf_dir;
                                  Arg.Set_string filename;
                                  Arg.Rest (fun is ->
                                      if String.length is > 0
                                      then end_i := (Some (Scanf.sscanf is "%Li" (fun i -> i))))
                                ],
     "<tlog_dir> <tlf_dir> <path-to-db> [<end-i>]");
    ("-dump-values", Arg.Set dump_values, "also dumps values (in --dump-tlog)");
    ("--make-tlog", Arg.Tuple[ set_laction MakeTlog;
                               Arg.Set_string filename;
                               Arg.Set_int counter;],
     "<filename> <counter> : make a tlog file with 1 NOP entry @ <counter>");
    ("--dump-store", Arg.Tuple [ set_laction DumpStore;
                                 Arg.Set_string filename],
     "<filename> : dump a store");
    ("--compress-tlog", Arg.Tuple[set_laction CompressTlog;
                                  Arg.Set_string filename],
     "<filename> : compress a tlog file");
    ("-archive", Arg.Set_string archive_type,
     "either '.tlf' or '.tls'");
    ("--uncompress-tlog", Arg.Tuple[set_laction UncompressTlog;
                                    Arg.Set_string filename],
     "<filename> : uncompress a tlog file");
    ("--set", Arg.Tuple [set_laction SET;
                         Arg.Set_string key;
                         Arg.Set_string value;
                        ], "<key> <value> : arakoon[<key>] = <value>");
    ("--get", Arg.Tuple [set_laction GET;
                         Arg.Set_string key
                        ],"<key> : arakoon[<key>]");
    ("--delete", Arg.Tuple[set_laction DELETE;
                           Arg.Set_string key;
                          ], "<key> : delete arakoon[<key>]");
    ("--delete-prefix", Arg.Tuple[set_laction DELETE_PREFIX;
                                  Arg.Set_string key;
                                 ], "<prefix> : delete all entries where the key matches <prefix>");
    ("--prefix", Arg.Tuple[set_laction PREFIX;
                           Arg.Set_string key;
                          ], "<prefix>: all starting with <prefix>");
    ("--benchmark", set_laction BENCHMARK, "run a benchmark on an existing Arakoon cluster");
    ("--load", Arg.Tuple [set_laction LOAD;Arg.Set_int n_clients],
     "<n> clients that generate load on a cluster");
    ("--who-master", Arg.Tuple[set_laction WHO_MASTER;], "tells you who's the master");
    ("--expect-progress-possible", Arg.Tuple[set_laction EXPECT_PROGRESS_POSSIBLE;],
     "tells you if the master thinks progress is possible");
    ("--statistics", set_laction STATISTICS, "returns some master statistics");
    ("--run-system-tests", set_laction SystemTests,
     "run system tests (you need a running installation)");
    ("--version", set_laction ShowVersion, "shows version");
    (* ("-port", Arg.Set_int port, "specifies server port"); *)
    ("-config", Arg.Set_string config_file,
     "specifies config file (default = cfg/arakoon.ini )");
    ("-catchup-only", Arg.Set catchup_only,
     "will only do a catchup of the node, without actually starting it (option to --node)");
    ("-daemonize", Arg.Set daemonize,
     "add if you want the process to daemonize (only for --node)");
    ("-start", Arg.Unit (fun () -> ()),
     "no-op for process-matching purposes");
    ("-value_size", Arg.Set_int value_size, "size of the values (only for --benchmark)");
    ("-key_size", Arg.Set_int key_size,
     "size of the keys (only for --benchmark)");
    ("-tx_size", Arg.Set_int tx_size, "size of transactions (only for --benchmark)");
    ("-max_n", Arg.Set_int max_n,     "<benchmark size> (only for --benchmark)");
    ("-n_clients", Arg.Set_int n_clients, "<n_clients>  (only for --benchmark)");
    ("-scenario", Arg.Set_string scenario,
     Printf.sprintf
       "(only for --benchmark) which scenario to run, default being %S"
       !scenario);
    ("-max_results", Arg.Set_int max_results, "max size of the result (for --prefix)");
    ("--test-repeat", Arg.Set_int test_repeat_count, "<repeat_count>");
    ("--collapse-remote", Arg.Tuple[set_laction Collapse_remote;
                                    Arg.Set_string cluster_id;
                                    Arg.Set_string ip;
                                    Arg.Set_int port;
                                    Arg.Set_int n_tlogs;
                                   ],
     "<cluster_id> <ip> <port> <n> tells node to collapse all but <n> tlogs into its head database");
    ("--nursery-init", Arg.Tuple[set_laction InitNursery;
                                 Arg.Set_string cluster_id;
                                ],
     "<cluster_id> Initialize the routing to contain a single cluster");
    ("--nursery-migrate", Arg.Tuple[set_laction MigrateNurseryRange;
                                    Arg.Set_string left_cluster;
                                    Arg.Set_string separator;
                                    Arg.Set_string right_cluster; ],
     "<left_cluster> <separator> <right_cluster> migrate a range by either adding a new cluster or modifying an existing separator between two cluster ranges");
    ("--nursery-delete", Arg.Tuple[set_laction DeleteNurseryCluster;
                                   Arg.Set_string cluster_id;
                                   Arg.Rest (fun s -> separator := s);
                                  ],
     "<cluster_id> <separator> removes <cluster_id> from the nursery, if the cluster is a boundary cluster no separator is required");
    ("--backup-db", Arg.Tuple[set_laction Backup_db;
                              Arg.Set_string cluster_id;
                              Arg.Set_string ip;
                              Arg.Set_int port;
                              Arg.Set_string location;
                             ],
     "<cluster_id> <ip> <port> <location> requests the node to stream over its database (only works on slaves)");
    ("--optimize-db", Arg.Tuple[set_laction Optimize_db;
                                Arg.Set_string cluster_id;
                                Arg.Set_string ip;
                                Arg.Set_int port;
                               ],
     "<cluster_id> <ip> <port> requests the node to optimize its database (only works on slaves)");
    ("--defrag-db", Arg.Tuple[set_laction Defrag_db;
                              Arg.Set_string cluster_id;
                              Arg.Set_string ip;
                              Arg.Set_int port;
                             ],
     "<cluster_id> <ip> <port> requests the node to defragment its database");
    ("--n-values", set_laction NumberOfValues,
     "returns the number of values in the store");
    ("--ping", Arg.Tuple[set_laction PING;
                         Arg.Set_string cluster_id;
                         Arg.Set_string ip;
                         Arg.Set_int port;
                        ],
     "<cluster_id> <ip> <port> sends a ping to the node");
    ("--node-version", Arg.Tuple[set_laction NODE_VERSION;
                                 Arg.Set_string node_id],
     "<node> : returns the version of <node>");
    ("--node-state", Arg.Tuple [set_laction NODE_STATE;
                                Arg.Set_string node_id],
     "<node> : returns the state of <node>");
    ("--inject-as-head", Arg.Tuple [set_laction InjectAsHead;
                                    Arg.Set_string filename;
                                    Arg.Set_string node_id],
     "<head.db> <node_id>"
    );
    ("--force", Arg.Set force,
     "force injection of the new head, even when the current head is corrupted (only for --inject-as-head)");
    ("--inplace", Arg.Set in_place,
     "perform an in-place inject-as-head using rename (only for --inject-as-head)");
    ("--drop-master", Arg.Tuple [set_laction Drop_master;
                                 Arg.Set_string cluster_id;
                                 Arg.Set_string ip;
                                 Arg.Set_int port;
                                ],
     "<cluster_id> <ip> <port> requests the node to drop its master role");
    ("-tls-ca-cert", Arg.Set_string tls_ca_cert, "<path> TLS CA certificate");
    ("-tls-cert", Arg.Set_string tls_cert, "<path> Certificate to use for TLS connections");
    ("-tls-key", Arg.Set_string tls_key, "<path> Key to use for TLS connections");
    ("-tls-version", Arg.Set_string tls_version, "<[1.0]|1.1|1.2> TLS version to use for connections");
    ("--range-entries", Arg.Tuple [set_laction RANGE_ENTRIES;],
     "list entries within range");
    ("--rev-range-entries", Arg.Tuple [set_laction REV_RANGE_ENTRIES;],
     "reverse list entries within range");
    ("-left", Arg.String  (fun s -> left  := Some s), "left boundary (range query)");
    ("-right", Arg.String (fun s -> right := Some s), "right boundary (range query)");

  ] in

  let options = [] in
  let interface = actions @ options in

  let do_local ~tls = function
    | ShowUsage -> print_endline usage;0
    | RunAllTests -> run_all_tests ()
    | RunAllTestsXML -> run_all_tests_xml !xml_filename
    | RunSomeTests -> run_some_tests !test_repeat_count !filter
    | ListTests -> list_tests ();0
    | SystemTests -> run_system_tests()
    | ShowVersion -> show_version();0
    | DumpTlog -> Tlog_main.dump_tlog !filename ~values:!dump_values
    | StripTlog -> Tlog_main.strip_tlog !filename
    | MakeTlog -> Tlog_main.make_tlog !filename !counter
    | MarkTlog -> Tlog_main.mark_tlog !filename !key
    | CloseTlog -> Tlog_main.mark_tlog !filename (Tlc2._make_close_marker !node_id)
    | ReplayTlogs -> Replay_main.replay_tlogs !tlog_dir !tlf_dir !filename !end_i
    | DumpStore -> Dump_store.dump_store !filename
    | TruncateTlog -> Tlc2.truncate_tlog !filename
    | CompressTlog -> Tlog_main.compress_tlog !filename !archive_type
    | UncompressTlog -> Tlog_main.uncompress_tlog !filename
    | SET -> Client_main.set ~tls !config_file !key !value
    | GET -> Client_main.get ~tls !config_file !key
    | PREFIX -> Client_main.prefix ~tls !config_file !key !max_results
    | DELETE_PREFIX -> Client_main.delete_prefix ~tls !config_file !key
    | BENCHMARK ->Client_main.benchmark ~tls !config_file
      !key_size !value_size !tx_size !max_n
      !n_clients !scenario
    | LOAD -> Load_client.main ~tls !config_file
                !n_clients
    | DELETE -> Client_main.delete ~tls !config_file !key
    | WHO_MASTER -> Client_main.who_master ~tls !config_file ()
    | EXPECT_PROGRESS_POSSIBLE -> Client_main.expect_progress_possible ~tls !config_file
    | STATISTICS -> Client_main.statistics ~tls !config_file
    | Collapse_remote -> Collapser_main.collapse_remote
                           ~tls !ip !port !cluster_id !n_tlogs
    | Backup_db -> Nodestream_main.get_db ~tls !ip !port !cluster_id !location
    | Optimize_db -> Nodestream_main.optimize_db ~tls !ip !port !cluster_id
    | Defrag_db   -> Nodestream_main.defrag_db ~tls !ip !port !cluster_id
    | NumberOfValues -> Client_main.get_key_count ~tls !config_file ()
    | InitNursery -> Nursery_main.init_nursery !config_file !cluster_id
    | MigrateNurseryRange -> Nursery_main.migrate_nursery_range
                               !config_file !left_cluster !separator !right_cluster
    | DeleteNurseryCluster -> Nursery_main.delete_nursery_cluster !config_file !cluster_id !separator
    | PING -> Client_main.ping ~tls !ip !port !cluster_id
    | NODE_VERSION -> Client_main.node_version ~tls !node_id !config_file
    | NODE_STATE   -> Client_main.node_state ~tls !node_id !config_file
    | InjectAsHead -> Dump_store.inject_as_head !filename !node_id !config_file
                        ~force:(!force) ~in_place:(!in_place)
    | Drop_master -> Nodestream_main.drop_master ~tls !ip !port !cluster_id
    | RANGE_ENTRIES ->
       Client_main.range_entries
         ~tls !config_file
         !left !linc !right !rinc !max_results
    | REV_RANGE_ENTRIES ->
       Client_main.rev_range_entries
         ~tls !config_file
         !left !linc !right !rinc !max_results

  in
  let do_server node =
    match node with
      | Node ->
        begin
          let canonical =
            if !config_file.[0] = '/'
            then !config_file
            else Filename.concat (Unix.getcwd()) !config_file
          in
          let make_config () = Node_cfg.read_config canonical in
          Daemons.maybe_daemonize !daemonize make_config;
          let main_t = (Node_main.main_t make_config
                          !node_id !daemonize !catchup_only)
          in
          (* Lwt_engine.set (new Lwt_engine.select :> Lwt_engine.t); *)
          Lwt_main.run main_t
        end
      | TestNode ->
        begin
          let lease_period = 60 in
          let node = Master_type.Forced "t_arakoon_0" in
          let make_config () = Node_cfg.make_test_config 3 node lease_period in
          let main_t = (Node_main.test_t make_config !node_id ~stop:(ref false)) in
          Lwt_main.run main_t
        end
  in
  Arg.parse
    interface
    (fun x -> raise (Arg.Bad ("Bad argument : " ^ x)))
    usage;

  let tls =
    if !tls_ca_cert = ""
      then None
      else begin
        let ca_cert = !tls_ca_cert
        and protocol = match !tls_version with
          | "1.0" -> Ssl.TLSv1
          | "1.1" -> Ssl.TLSv1_1
          | "1.2" -> Ssl.TLSv1_2
          | _ -> failwith "Invalid \"tls-version\" value"
        and creds = match !tls_cert with
          | "" -> None
          | s -> Some (s, !tls_key)
        in
        let ctx = Client_main.default_create_client_context ~ca_cert ~creds ~protocol in
        Some ctx
      end
  in

  let exit_code =
    match !action with
      | LocalAction la -> do_local ~tls la
      | ServerAction sa -> do_server sa
  in
  (* let () = Printf.printf "[rc=%i]\n" rc in *)
  exit exit_code
