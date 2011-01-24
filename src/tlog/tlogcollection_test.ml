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
open Extra
open Lwt
open Update
open Tlogcollection
open Tlogcommon

let setup factory () =
  Lwt_log.info "setup" >>= fun () ->
  let dn = "/tmp/tlogcollection" in
  let _ = Sys.command (Printf.sprintf "rm -rf '%s'" dn) in 
  let () = Unix.mkdir dn 0o755 in
  Lwt.return (dn, factory)


let teardown (dn, factory) = 
  Lwt_log.info_f "teardown %s" dn >>= fun () ->
  Lwt.return ()


let test_empty_collection (dn, factory) =
  Lwt_log.info "test_empty_collection" >>= fun () ->
  factory dn >>= fun c ->
  c # validate ()  >>= fun (v,i) ->
  Lwt.return ( OUnit.assert_equal i None )

let _log_repeat tlc update n = 
  let rec loop i = 
    if i = (Sn.of_int n) then Lwt.return ()
    else
      begin
	tlc # log_update i update >>= fun wr_result ->
	loop (Sn.succ i)
      end
  in loop Sn.start 

let test_rollover (dn, factory) =
  Lwt_log.info "test_rollover" >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 5 in
  factory dn >>= fun c ->

  let update = Update.Set ("x","y") in
  _log_repeat c update 101 >>= fun () ->
  c # close () >>= fun ()->
  Lwt.return ()


let test_rollover_1002 (dn, factory) =
  Lwt_log.info "test_rollover_1002" >>= fun () ->
  let n = 5 in
  let () = Tlogcommon.tlogEntriesPerFile := n in
  factory dn >>= fun c ->
  let update = Update.Set("x","y") in
  let n_updates = 1002 * 5 + 3 in
  _log_repeat c update n_updates >>= fun () ->
  c # close () >>= fun () ->
  factory dn >>= fun tlc_two ->
  tlc_two # get_last_update (Sn.of_int (n_updates-1)) >>= fun uo ->
  let uos = Log_extra.option_to_string Update.string_of uo in
  Lwt_log.info_f "last_update = %s" uos >>= fun () -> 
  tlc_two # close() >>= fun () ->
  Lwt.return ()


let test_get_value_bug (dn, factory) = 
  Lwt_log.info "test_get_value_bug" >>= fun () ->
  factory dn >>= fun c0 ->
  let u0 = Update.make_master_set "XXXX" None in
  c0 # log_update 0L u0 >>= fun wr_result ->
  factory dn >>= fun c1 ->
  (* c1 # validate () >>= fun _ -> *)
  c1 # get_last_update 0L >>= function
    | None -> Llio.lwt_failfmt "get_last_update 0 yields None"
    | Some u -> let () = OUnit.assert_equal u u0 in Lwt.return ()

let test_regexp (dn,factory) = 
  Lwt_log.info "test_get_regexp_bug" >>= fun () ->
  let fns = ["001.tlog";"000.tlc";"000.tlc.part"] in
  let test fn = Str.string_match Tlc2.file_regexp fn 0 in
  let results = List.map test fns in
  List.iter2 (fun cr er -> OUnit.assert_equal cr er) results [true;true;false];
  Lwt.return ()

let test_restart (dn, factory) =
  factory dn >>= fun tlc_one ->
  let update = Update.Set("x","y") in
  _log_repeat tlc_one update 100 >>= fun () ->
  tlc_one # close () >>= fun () ->
  factory dn >>= fun tlc_two ->
  tlc_two # get_last_update (Sn.of_int 99) >>= fun uo ->
  Lwt.return ()



let test_iterate (dn, factory) =
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn >>= fun  tlc ->
  let update = Update.Set("xxx","y")in
  _log_repeat tlc update 323 >>= fun () ->
  let sum = ref 0 in
  tlc # iterate (Sn.of_int 125) (Sn.of_int 303)
    (fun (i,u) -> sum := !sum + (Int64.to_int i); 
      Lwt_log.debug_f"i=%s" (Sn.string_of i) >>= fun () ->
      Lwt.return ())
  >>= fun () ->
  tlc # close () >>= fun () ->
  Lwt_log.debug_f "sum =%i " !sum >>= fun () ->
  OUnit.assert_equal !sum 38306;
  Lwt.return ()


let test_iterate2 (dn, factory) = 
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn >>= fun tlc ->
  let update = Update.Set("test_iterate0","xxx") in
  _log_repeat tlc update 1 >>= fun () ->
  let result = ref [] in
  tlc # iterate (Sn.of_int 0) (Sn.of_int 0) 
    (fun (i,u) -> result := i :: ! result; 
     Lwt_log.debug_f "i=%s" (Sn.string_of i) >>= fun () ->
     Lwt.return ())
  >>= fun () -> 
  OUnit.assert_equal (List.length !result) 1;
  Lwt.return ()


let test_iterate3 (dn,factory) = 
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn >>= fun tlc ->
  let update = Update.Set("test_iterate3","xxx") in
  _log_repeat tlc update 120 >>= fun () ->
  let result = ref [] in
  tlc # iterate (Sn.of_int 99) (Sn.of_int 101)
    (fun (i,u) -> 
      Lwt_log.debug_f "i=%s" (Sn.string_of i) >>= fun () ->
      let () = result := i :: !result in
      Lwt.return ()
    )
  >>= fun () ->
  OUnit.assert_equal (List.mem (Sn.of_int 99) !result) true;
  Lwt.return ()

let test_validate_normal (dn, factory) = 
  let () = Tlogcommon.tlogEntriesPerFile:= 100 in
  factory dn >>= fun (tlc:tlog_collection) ->
  let update = Update.Set ("XXX","X") in
  _log_repeat tlc update 123 >>= fun () ->
  tlc # close () >>= fun () ->
  factory dn >>= fun (tlc_two:tlog_collection) ->
  tlc_two # validate_last_tlog () >>= fun result ->
  let validity, io = result in
  let wsn = Sn.of_int 122 in
  let wanted = (Some wsn) in
  let tos x= Log_extra.option_to_string Sn.string_of x in
  Lwt_log.info_f "wanted:%s, got:%s" (tos wanted) (tos io)
  >>= fun() ->
  OUnit.assert_equal io wanted ;
  Lwt.return ()

let test_validate_corrupt_1 (dn,factory) =
  let () = Tlogcommon.tlogEntriesPerFile:= 100 in
  factory dn >>= fun (tlc:tlog_collection) -> 
  let update = Update.Set("Incompetent","Politicians") in
  _log_repeat tlc update 42 >>= fun () ->
  tlc # close () >>= fun () ->
  let fn = Filename.concat dn "000.tlog" in
  let fd = Lwt_unix.openfile fn [Unix.O_RDWR] 0o640 in
  let _ = Lwt_unix.lseek fd 666 Unix.SEEK_SET in
  Lwt_unix.write fd "\x00\x00\x00\x00\x00\x00" 0 6 >>= fun _ ->
  Lwt_unix.close fd;
  Lwt_log.info_f "corrupted 6 bytes" >>= fun () ->
  Lwt.catch
    (fun () -> 
      factory dn >>= fun (tlc_two:tlog_collection) ->
      tlc_two # validate_last_tlog () >>= fun _ -> 
      tlc_two # close () >>= fun () ->
      OUnit.assert_bool "this tlog should not be valid" false;
      Lwt.return ()
    )
    (function
      | Tlc2.TLCCorrupt (pos,i) -> Lwt.return ()
      | exn -> 
	let msg = Printf.sprintf "it threw the wrong exception %s" "?" in
	OUnit.assert_bool msg false;
	Lwt.return ()
    )
  >>= fun () -> 
  Lwt.return ()

let wrap factory test = lwt_bracket (setup factory) test teardown
let wrap_file = wrap make_file_tlog_collection
let wrap_memory = wrap Mem_tlogcollection.make_mem_tlog_collection

let suite_mem = "mem_tlogcollection" >::: [
  "empty_collection" >:: wrap_memory test_empty_collection;
  "rollover" >:: wrap_memory test_rollover;
(* "get_value_bug" >:: wrap_memory test_get_value_bug; 
    (* assumption that different tlog_collections with the same name have the same state *) 
*)
]

let suite_file = "file_tlogcollection" >:::[
  "empty_collection" >:: wrap_file test_empty_collection;
  "rollover" >:: wrap_file test_rollover;
  "get_value_bug" >:: wrap_file test_get_value_bug;
  "restart" >:: wrap_file test_restart;
  "iterate" >:: wrap_file test_iterate;
  "iterate2">:: wrap_file test_iterate2;
  "validate" >:: wrap_file test_validate_normal;
]


