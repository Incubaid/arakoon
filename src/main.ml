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

open Lwt
open Version
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
  | DumpStore
  | MakeTlog
  | TruncateTlog
  | CompressTlog
  | UncompressTlog
  | BENCHMARK
  | SET
  | GET
  | DELETE
  | WHO_MASTER
  | EXPECT_PROGRESS_POSSIBLE
  | STATISTICS
  | Collapse
  | Collapse_remote

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
  Printf.printf "version: %S\n" Version.version;
  Printf.printf "hg_revision: %S\n" Version.hg_revision;
  Printf.printf "compiled: %S\n" Version.compile_time;
  Printf.printf "tlogEntriesPerFile: %i\n" !Tlogcommon.tlogEntriesPerFile

let run_all_tests () =
  All_test.configure_logging();
  let tests = All_test.suite in
  let _ = OUnit.run_test_tt tests in 0

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
  let (_:test_result list) = perform_timed_tests result tests in
  let () = print_xml result filename in
  0

let run_system_tests () =
  All_test.configure_logging();
  let tests = System_tests.suite in
  let _ = OUnit.run_test_tt tests in
  0

let dump_tlog filename =
  let printer () (i,u) = 
    Lwt_io.printlf "%s:%s" (Sn.string_of i) (Update.Update.string_of u) in
  let folder,_ = Tlc2.folder_for filename in

  let t =
      begin
	let do_it ic =
	  let lowerI = Sn.start
	  and higherI = None 
	  and first = Sn.of_int 0 
	  and a0 = () in
	  folder ic lowerI higherI ~first a0 printer >>= fun () ->
	  Lwt.return 0
	in
	Lwt_io.with_file ~mode:Lwt_io.input filename do_it
      end
  in
  Lwt_main.run t

let make_tlog tlog_name (i:int) =
  let sni = Sn.of_int i in
  let t = 
    let f oc = Tlogcommon.write_entry oc sni Update.Update.Nop
    in
    Lwt_io.with_file ~mode:Lwt_io.output tlog_name f
  in
  Lwt_main.run t;0

let dump_store filename = Dump_store.dump_store filename

   
let compress_tlog tlu =
  let tlf = Tlc2.to_archive_name tlu in
  let t = Compression.compress_tlog tlu tlf in
  Unix.unlink tlu;
  Lwt_main.run t;0
    
let uncompress_tlog tlx =
  let t = 
    let extension = Tlc2.extension_of tlx in
    if extension = Tlc2.archive_extension then
      begin
	let tlu = Tlc2.to_tlog_name tlx in
	Compression.uncompress_tlog tlx tlu >>= fun () ->
	Unix.unlink tlx;
	Lwt.return ()
      end
    else if extension = ".tlc" then
      begin
	let tlu = Tlc2.to_tlog_name tlx in
	Tlc_compression.tlc2tlog tlx tlu >>= fun () ->
	Unix.unlink tlx;
	Lwt.return ()
      end
    else Lwt.fail (Failure "unknown file format")
  in
  Lwt_main.run t;0
  
let run_some_tests repeat_count filter =
  All_test.configure_logging();
  Printf.printf "running tests matching '%s'\n" filter;
  let parts = Str.split_delim (Str.regexp "\\.") filter in
  let show_count num =
    if repeat_count > 1 then Printf.printf "-- iteration %d\n" (repeat_count - num + 1)
  in
  let rec do_n_times f = function
    | 0 -> ()
    | n ->
      let () = show_count n in
      let (_:OUnit.test_result list) = f () in
      do_n_times f (n-1)
  in
  match OUnit.test_filter parts All_test.suite  with
    | Some test ->
      begin
	List.iter do_path (OUnit.test_case_paths test);
	let () = do_n_times (fun () -> OUnit.run_test_tt_main test) repeat_count in
	0
      end

    | None -> failwith (Printf.sprintf "no test matches '%s'" filter);;

let () = Sys.set_signal Sys.sigpipe Sys.Signal_ignore in
let () = Random.self_init () in
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
and size = ref 10 
and tx_size = ref 100
and daemonize = ref false
and test_repeat_count = ref 1
and counter = ref 0
and n_tlogs = ref 1
and catchup_only = ref false
and tlog_dir = ref "/tmp"
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
  ("--make-tlog", Arg.Tuple[ set_laction MakeTlog;
			     Arg.Set_string filename;
			     Arg.Set_int counter;],
   "<filename> <counter> : make a tlog file with 1 NOP entry @ <counter>");
  ("--dump-store", Arg.Tuple [ set_laction DumpStore; 
			       Arg.Set_string filename],
   "<filename> : dump a store");
  ("--compress-tlog", Arg.Tuple[set_laction CompressTlog;
				Arg.Set_string filename],
   "compress a tlog file");
  ("--uncompress-tlog", Arg.Tuple[set_laction UncompressTlog;
				  Arg.Set_string filename],
   "uncompress a tlog file");
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
  ("--benchmark", set_laction BENCHMARK, "run a benchmark on an existing Arakoon cluster");
  ("--who-master", Arg.Tuple[set_laction WHO_MASTER;], "tells you who's the master");
  ("--expect-progress-possible", Arg.Tuple[set_laction EXPECT_PROGRESS_POSSIBLE;],
   "tells you if the master thinks progress is possible");
  ("--statistics", set_laction STATISTICS, "returns some master statistics");
  ("--run-system-tests", set_laction SystemTests,
   "run system tests (you need a running installation");
  ("--version", set_laction ShowVersion, "shows version");
  (* ("-port", Arg.Set_int port, "specifies server port"); *)
  ("-config", Arg.Set_string config_file,
   "specifies config file (default = cfg/arakoon.ini )");
  ("-catchup-only", Arg.Set catchup_only,
   "will only do a catchup of the node, without actually starting it (option to --node)");
  ("-daemonize", Arg.Set daemonize,
   "add if you want the process to daemonize (only for --node)");
  ("-value_size", Arg.Set_int size,
   "size of the values (only for --benchmark)");
  ("-tx_size", Arg.Set_int tx_size,
   "size of transactions (only for --benchmark)");
  ("--test-repeat", Arg.Set_int test_repeat_count, "<repeat_count>");
  ("--collapse", Arg.Tuple[set_laction Collapse;
			   Arg.Set_string tlog_dir;
			   Arg.Set_int n_tlogs;],   
   "<tlog_dir> <n> collapses n tlogs from <tlog_dir> into head database");
  ("--collapse-remote", Arg.Tuple[set_laction Collapse_remote;
				  Arg.Set_string cluster_id;
				  Arg.Set_string ip;
				  Arg.Set_int port;
				  Arg.Set_int n_tlogs;
				 ], 
   "<cluster_id> <ip> <port> <n> tells node to collapse <n> tlogs into its head database");

  
] in

let options = [] in
let interface = actions @ options in

let do_local = function
  | ShowUsage -> print_endline usage;0
  | RunAllTests -> run_all_tests ()
  | RunAllTestsXML -> run_all_tests_xml !xml_filename
  | RunSomeTests -> run_some_tests !test_repeat_count !filter
  | ListTests -> list_tests ();0
  | SystemTests -> run_system_tests()
  | ShowVersion -> show_version();0
  | DumpTlog -> dump_tlog !filename
  | MakeTlog -> make_tlog !filename !counter
  | DumpStore -> dump_store !filename
  | TruncateTlog -> Tlc2.truncate_tlog !filename
  | CompressTlog -> compress_tlog !filename
  | UncompressTlog -> uncompress_tlog !filename
  | SET -> Client_main.set !config_file !key !value
  | GET -> Client_main.get !config_file !key
  | BENCHMARK -> Client_main.benchmark !config_file !size !tx_size
  | DELETE -> Client_main.delete !config_file !key
  | WHO_MASTER -> Client_main.who_master !config_file ()
  | EXPECT_PROGRESS_POSSIBLE -> Client_main.expect_progress_possible !config_file
  | STATISTICS -> Client_main.statistics !config_file
  | Collapse -> Collapser_main.collapse !tlog_dir !n_tlogs
  | Collapse_remote -> Collapser_main.collapse_remote 
    !ip !port !cluster_id !n_tlogs

in
let do_server node =
  match node with
    | Node ->
      let make_config () = Node_cfg.read_config !config_file in
      let main_t = (Node_main.main_t make_config !node_id !daemonize !catchup_only) in
      Lwt_main.run main_t;
      0
    | TestNode ->
      let lease_period = 60 in 
      let node = Master_type.Forced "t_arakoon_0" in
      let make_config () = Node_cfg.make_test_config 3 node lease_period in
      let main_t = (Node_main.test_t make_config !node_id) in
      Lwt_main.run main_t;
      0

in
Arg.parse
  interface
  (fun x -> raise (Arg.Bad ("Bad argument : " ^ x)))
  usage;
let res =
  match !action with
    | LocalAction la -> do_local la
    | ServerAction sa -> do_server sa
in
exit res;;

