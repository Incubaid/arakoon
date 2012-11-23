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


let setup factory test_name () =
  let dn = Printf.sprintf "/tmp/%s" test_name in
  Lwt_log.info_f "setup %s" dn >>= fun () ->
  File_system.exists dn >>= (function
    | true -> 
      begin
        Lwt_log.info_f "%s exists cleaning" dn >>= fun () ->
        let cmd = Lwt_process.shell (Printf.sprintf "rm -rf %s" dn) in
        Lwt_process.exec cmd
        >>= fun status ->
        begin
          match status with
            | Unix.WEXITED rc when rc = 0 -> File_system.mkdir dn 0o755
            | Unix.WEXITED rc             -> Llio.lwt_failfmt "rm -rf '%s' gave rc %i" dn rc
            | _                           -> Llio.lwt_failfmt "rm -rf '%s' failed" dn
        end

      end
    | false -> File_system.mkdir dn 0o755
  ) >>= fun () ->
  Lwt.return (dn, factory)      
  


let teardown (dn, factory) = Lwt_log.info_f "teardown %s" dn 

let _make_set_v k v= Value.create_client_value [Update.Set (k,v)] false

let _log_repeat tlc (value:Value.t) n = 
  let sync = false in
  let rec loop i = 
    if i = (Sn.of_int n) then Lwt.return ()
    else
      begin
	tlc # log_value i value ~sync >>= fun wr_result ->
	loop (Sn.succ i)
      end
  in loop Sn.start 

let test_rollover (dn, factory) =
  Lwt_log.info "test_rollover" >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 5 in
  factory dn >>= fun c ->
  let value = _make_set_v "x" "y" in
  _log_repeat c value 101 >>= fun () ->
  c # close () >>= fun ()->
  Lwt.return ()


let test_rollover_1002 (dn, factory) =
  Lwt_log.info "test_rollover_1002" >>= fun () ->
  let n = 5 in
  let () = Tlogcommon.tlogEntriesPerFile := n in
  factory dn >>= fun c ->
  let value = _make_set_v "x" "y" in
  let n_updates = 1002 * n + 3 in
  _log_repeat c value n_updates >>= fun () ->
  c # close () >>= fun () ->
  factory dn >>= fun tlc_two ->
  let vo = tlc_two # get_last_value (Sn.of_int (n_updates-1)) in
  let vos = Log_extra.option2s Value.value2s vo in
  Lwt_log.info_f "last_value = %s" vos >>= fun () -> 
  tlc_two # close() >>= fun () ->
  Lwt.return ()


let test_get_value_bug (dn, factory) = 
  Lwt_log.info "test_get_value_bug" >>= fun () ->
  factory dn >>= fun c0 ->
  let v0 = Value.create_master_value ("XXXX",0L) in
  c0 # log_value 0L v0 ~sync:false >>= fun wr_result ->
  factory dn >>= fun c1 ->
  (* c1 # validate () >>= fun _ -> *)
  match c1 # get_last_value 0L with
    | None -> Llio.lwt_failfmt "get_last_update 0 yields None"
    | Some v -> let () = OUnit.assert_equal v v0 in Lwt.return ()

let test_regexp (dn,factory) = 
  Lwt_log.info "test_get_regexp_bug" >>= fun () ->
  let fns = ["001.tlog";
	     "000" ^ Tlc2.archive_extension;
	     "000" ^ Tlc2.archive_extension ^ ".part"] in
  let test fn = Str.string_match Tlc2.file_regexp fn 0 in
  let results = List.map test fns in
  List.iter2 (fun cr er -> OUnit.assert_equal cr er) results [true;true;false];
  Lwt.return ()

let test_restart (dn, factory) =
  factory dn >>= fun tlc_one ->
  let value = _make_set_v "x" "y" in
  _log_repeat tlc_one value 100 >>= fun () ->
  tlc_one # close () >>= fun () ->
  factory dn >>= fun tlc_two ->
  let uo = tlc_two # get_last_value (Sn.of_int 99) in
  tlc_two # close () >>= fun () ->
  Lwt.return ()



let test_iterate (dn, factory) =
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn >>= fun  tlc ->
  let value = _make_set_v "xxx" "y" in
  _log_repeat tlc value 323 >>= fun () ->
  let sum = ref 0 in
  tlc # iterate (Sn.of_int 125) (Sn.of_int 304)
    (fun entry -> 
      let i = Entry.i_of entry in
      sum := !sum + (Int64.to_int i); 
      Lwt_log.debug_f"i=%s" (Sn.string_of i) >>= fun () ->
      Lwt.return ())
  >>= fun () ->
  tlc # close () >>= fun () ->
  Lwt_log.debug_f "sum =%i " !sum >>= fun () ->
  OUnit.assert_equal ~printer:string_of_int !sum 38306;
  Lwt.return ()


let test_iterate2 (dn, factory) = 
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn >>= fun tlc ->
  let value = _make_set_v "test_iterate0" "xxx" in
  _log_repeat tlc value 3 >>= fun () ->
  let result = ref [] in
  tlc # iterate (Sn.of_int 0) (Sn.of_int 1) 
    (fun entry -> 
      let i = Entry.i_of entry in
      result := i :: ! result; 
      Lwt_log.debug_f "i=%s" (Sn.string_of i) >>= fun () ->
      Lwt.return ())
  >>= fun () -> 
  OUnit.assert_equal ~printer:string_of_int 1 (List.length !result);
  tlc # close () >>= fun () ->
  Lwt.return ()


let test_iterate3 (dn,factory) = 
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn >>= fun tlc ->
  let value = _make_set_v "test_iterate3" "xxx" in
  _log_repeat tlc value 120 >>= fun () ->
  let result = ref [] in
  tlc # iterate (Sn.of_int 99) (Sn.of_int 101)
    (fun entry -> 
      let i = Entry.i_of entry in
      Lwt_log.debug_f "i=%s" (Sn.string_of i) >>= fun () ->
      let () = result := i :: !result in
      Lwt.return ()
    )
  >>= fun () ->
  OUnit.assert_equal (List.mem (Sn.of_int 99) !result) true;
  tlc # close () >>= fun () ->
  Lwt.return ()




let test_validate_normal (dn, factory) = 
  let () = Tlogcommon.tlogEntriesPerFile:= 100 in
  factory dn >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "XXX" "X" in
  _log_repeat tlc value 123 >>= fun () ->
  tlc # close () >>= fun () ->
  factory dn >>= fun (tlc_two:tlog_collection) ->
  tlc_two # validate_last_tlog () >>= fun result ->
  let validity, eo, _ = result in
  let wsn = Sn.of_int 122 in
  let wanted = (Some wsn) in
  let io = match eo with None -> None | Some e -> Some (Entry.i_of e) in
  let tos x= Log_extra.option2s Sn.string_of x in
  Lwt_log.info_f "wanted:%s, got:%s" (tos wanted) (tos io)
  >>= fun() ->
  OUnit.assert_equal io wanted ;
  Lwt.return ()

let test_validate_corrupt_1 (dn,factory) =
  let () = Tlogcommon.tlogEntriesPerFile:= 100 in
  factory dn >>= fun (tlc:tlog_collection) -> 
  let value = _make_set_v "Incompetent" "Politicians" in
  _log_repeat tlc value 42 >>= fun () ->
  tlc # close () >>= fun () ->
  let fn = Filename.concat dn "000.tlog" in
  Lwt_unix.openfile fn [Unix.O_RDWR] 0o640 >>= fun fd ->
  Lwt_unix.lseek fd 666 Unix.SEEK_SET >>= fun _ ->
  Lwt_unix.write fd "\x00\x00\x00\x00\x00\x00" 0 6 >>= fun _ ->
  Lwt_unix.close fd >>= fun () ->
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

let wrap factory test (name:string) = lwt_bracket (setup factory name) test teardown

let create_test_tlc dn = Mem_tlogcollection.make_mem_tlog_collection dn true 

let wrap_memory name = wrap create_test_tlc name

let suite_mem = "mem_tlogcollection" >::: [
  "rollover" >:: wrap_memory test_rollover "rollover";
(* "get_value_bug" >:: wrap_memory test_get_value_bug; 
    (* assumption that different tlog_collections with the same name have the same state *) 
*)
]



