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
open Update
open Extra

let wrap_tlc = Tlogcollection_test.wrap Tlc2.make_tlc2

let prepare_tlog_scenarios (dn,factory) =
  let old_tlog_entries_value = !Tlogcommon.tlogEntriesPerFile in
  Tlogcommon.tlogEntriesPerFile := 5 ;
  factory dn >>= fun tlog_coll ->
  let update = Update.make_master_set "me" None in
  tlog_coll # log_update 0L update >>= fun _ ->
  tlog_coll # log_update 1L update >>= fun _ ->
  tlog_coll # log_update 2L update >>= fun _ ->
  tlog_coll # log_update 3L update >>= fun _ ->
  tlog_coll # log_update 4L update >>= fun _ ->
  tlog_coll # close () >>= fun _ ->
  Lwt.return old_tlog_entries_value

let test_interrupted_rollover (dn,factory) =
  prepare_tlog_scenarios (dn,factory) >>= fun old_tlog_entries_value ->
  let fn = Filename.concat dn "001.tlog" in
  Unix.unlink fn;
  factory dn >>= fun tlog_coll ->
  let update = Update.make_master_set "me" None in
  tlog_coll # log_update 5L update >>= fun _ ->
  tlog_coll # close () >>= fun _ ->
  Tlc2.get_tlog_names dn >>= fun tlog_names ->
  let n = List.length tlog_names in
  Tlogcommon.tlogEntriesPerFile := old_tlog_entries_value;
  let msg = Printf.sprintf "Number of tlogs incorrect. Expected 2, got %d" n in
  Lwt.return (OUnit.assert_equal ~msg n 2) 
  

let test_validate_at_rollover_boundary (dn,factory) =
  prepare_tlog_scenarios (dn,factory) >>= fun old_tlog_entries_value ->
  factory dn >>= fun val_tlog_coll ->
  val_tlog_coll # validate_last_tlog () >>= fun (validity, lasti) ->
  let lasti_str = 
  begin
    match lasti with
      | None -> "None"
      | Some i -> Sn.string_of i
  end in
  val_tlog_coll # close () >>= fun _ ->
  let msg = Printf.sprintf "Values of is are different 4 <> %s" lasti_str in
  begin
    if lasti <> (Some 4L)
    then Tlogcommon.tlogEntriesPerFile := old_tlog_entries_value
  end;
  OUnit.assert_equal ~msg lasti (Some 4L) ;
  factory dn >>= fun tlog_coll ->
  let update = Update.make_master_set "me" None in
  tlog_coll # log_update 5L update >>= fun _ ->
  tlog_coll # log_update 6L update >>= fun _ ->
  tlog_coll # log_update 7L update >>= fun _ ->
  tlog_coll # log_update 8L update >>= fun _ ->
  tlog_coll # log_update 9L update >>= fun _ ->    
  Tlc2.get_tlog_names dn >>= fun tlog_names ->
  let n = List.length tlog_names in
  Tlogcommon.tlogEntriesPerFile := old_tlog_entries_value;
  let msg = Printf.sprintf "Number of tlogs incorrect. Expected 3, got %d" n in
  Lwt.return (OUnit.assert_equal ~msg n 3)

let test_iterate4 (dn, factory) =
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn >>= fun tlc ->
  let update = Update.Set("test_iterate4","xxx") in
  Tlogcollection_test._log_repeat tlc update 120 >>= fun () ->
  Lwt_unix.sleep 2.0 >>= fun () -> (* compression should have callback *)
  let fnc = Filename.concat dn "000.tlc" in
  Unix.unlink fnc;
  (* remove 000.tlog & 000.tlc ; errors? *)
  tlc # get_infimum_i () >>= fun inf ->
  Lwt_log.debug_f "inf=%s" (Sn.string_of inf) >>= fun () ->
  OUnit.assert_equal ~printer:Sn.string_of inf (Sn.of_int 100);
  Lwt.return ()


let suite = "tlc2" >:::[
  "regexp" >:: wrap_tlc Tlogcollection_test.test_regexp;
  "empty_collection" >:: wrap_tlc Tlogcollection_test.test_empty_collection;
  "rollover" >:: wrap_tlc Tlogcollection_test.test_rollover;
  "get_value_bug" >:: wrap_tlc Tlogcollection_test.test_get_value_bug;
  "test_restart" >:: wrap_tlc Tlogcollection_test.test_restart;
  "test_iterate" >:: wrap_tlc Tlogcollection_test.test_iterate;
  "test_iterate2" >:: wrap_tlc Tlogcollection_test.test_iterate2;
  "test_iterate3" >:: wrap_tlc Tlogcollection_test.test_iterate3;
  "test_iterate4" >:: wrap_tlc test_iterate4;
  "validate" >:: wrap_tlc Tlogcollection_test.test_validate_normal;
  "validate_corrupt" >:: wrap_tlc Tlogcollection_test.test_validate_corrupt_1;
  "test_rollover_1002" >:: wrap_tlc Tlogcollection_test.test_rollover_1002;
  "test_rollover_boundary" >:: wrap_tlc test_validate_at_rollover_boundary;
  "test_interrupted_rollover" >:: wrap_tlc test_interrupted_rollover;
]
