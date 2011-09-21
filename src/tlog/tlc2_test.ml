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
open Unix
open Lwt
open Update
open Extra

let create_test_tlc dn = Tlc2.make_tlc2 dn true
let wrap_tlc = Tlogcollection_test.wrap create_test_tlc 

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
  (*let fn = Filename.concat dn "001.tlog" in
  Unix.unlink fn; *)
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
  let msg = Printf.sprintf "Number of tlogs incorrect. Expected 2, got %d" n in
  Lwt.return (OUnit.assert_equal ~msg n 2)

let test_iterate4 (dn, factory) =
  Lwt_log.debug "test_iterate4" >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn >>= fun tlc ->
  let update = Update.Set("test_iterate4","xxx") in
  Tlogcollection_test._log_repeat tlc update 120 >>= fun () ->
  Lwt_unix.sleep 3.0 >>= fun () -> (* compression should have callback *)
  let fnc = Filename.concat dn ("000" ^ Tlc2.archive_extension) in
  Unix.unlink fnc;
  (* remove 000.tlog & 000.tlf ; errors? *)
  tlc # get_infimum_i () >>= fun inf ->
  Lwt_log.debug_f "inf=%s" (Sn.string_of inf) >>= fun () ->
  OUnit.assert_equal ~printer:Sn.string_of inf (Sn.of_int 100);
  Lwt_log.debug_f "end of test_iterate4" >>= fun () -> 
  Lwt.return ()


let test_iterate5 (dn,factory) = 
  let () = Tlogcommon.tlogEntriesPerFile := 10 in
  factory dn >>= fun tlc ->
  let rec loop tlc i = 
    if i = 33 
    then Lwt.return ()
    else
      begin
        let is = string_of_int i in
        let update = Update.Set("test_iterate_" ^ is ,is) in
        tlc # log_update (Sn.of_int i) update >>= fun _ ->
          begin
            if i mod 3 = 2 
            then 
              begin
                tlc # close () >>= fun () ->
                factory dn
              end
            else Lwt.return tlc
          end
        >>= fun tlc' ->
        loop tlc' (i+1)
      end
  in
  loop tlc 0 >>= fun () ->
  let start_i = Sn.of_int 10 in
  let too_far_i = Sn.of_int 11 in
  let f (i,u) = Lwt_log.debug_f "test_iterate5: %s %s" (Sn.string_of i) 
    (Update.string_of u) 
  in
  tlc # iterate start_i too_far_i f >>= fun () ->
  Lwt.return () 

let test_iterate6 (dn,factory) = 
  let () = Tlogcommon.tlogEntriesPerFile := 10 in
  factory dn >>= fun tlc ->
  let rec loop i = 
    if i = 33 
    then Lwt.return ()
    else
      begin
        let is = string_of_int i in
	let sni = Sn.of_int i in
        let update = Update.Set("test_iterate_" ^ is ,is) in
          begin
            if i != 19
            then 
              tlc # log_update sni update 
            else
	      begin
		tlc # log_update sni update >>= fun _ ->
		let u2 = Update.Set("something_else","gotcha") in
		tlc # log_update sni u2
	      end
          end >>= fun _ ->
        loop (i+1)
      end
  in
  loop 0 >>= fun () ->
  let sum = ref 0 in
  let start_i = Sn.of_int 19 in
  let too_far_i = Sn.of_int 20 in
  tlc # iterate start_i too_far_i
    (fun (i,u) -> sum := !sum + (Int64.to_int i); 
      Lwt_log.debug_f "i=%s : %s" 
	(Sn.string_of i) (Update.string_of u)
      >>= fun () ->
      Lwt.return ())
  >>= fun () ->
  tlc # close () >>= fun () ->
  Lwt_log.debug_f "sum =%i " !sum >>= fun () ->
  OUnit.assert_equal ~printer:string_of_int 19 !sum;
  Lwt.return () 


let test_compression_bug (dn, factory) =
  let () = Tlogcommon.tlogEntriesPerFile := 10 in
  let v = String.create (1024 * 1024) in
  factory dn >>= fun tlc ->
  let n = 12 in
  let rec loop i = 
    if i = n then Lwt.return () 
    else
      let key = Printf.sprintf "test_compression_bug_%i" i in
      let update = Update.Set(key, v) in
      let sni = Sn.of_int i in
      tlc # log_update sni update >>= fun () ->
      loop (i+1) 
  in
  tlc # log_update 0L (Update.Set("xxx","XXX")) >>= fun () ->
  loop 1 >>= fun () ->
  tlc # close () >>= fun () ->
  begin
    if !Compression.jobs > 0 
    then Lwt_condition.wait Compression.jobs_condition 
    else Lwt.return () 
  end
  >>= fun () ->
  File_system.stat (dn ^ "/000.tlf") >>= fun stat ->
  OUnit.assert_bool "file should have size >0" (stat.st_size > 0);
  let entries = ref [] in
  factory dn >>= fun tlc2 ->
  tlc2 # iterate 0L (Sn.of_int n)   
    (fun (i,u) -> entries := i :: !entries;
      Lwt_log.debug_f "ENTRY: i=%Li" i) 
  >>= fun () ->
  OUnit.assert_equal 
    ~printer:string_of_int 
    ~msg:"tlc has a hole" n (List.length !entries);
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
  "test_iterate5" >:: wrap_tlc test_iterate5;
  "test_iterate6" >:: wrap_tlc test_iterate6;
  "validate" >:: wrap_tlc Tlogcollection_test.test_validate_normal;
  "validate_corrupt" >:: wrap_tlc Tlogcollection_test.test_validate_corrupt_1;
  "test_rollover_1002" >:: wrap_tlc Tlogcollection_test.test_rollover_1002;
  "test_rollover_boundary" >:: wrap_tlc test_validate_at_rollover_boundary;
  "test_interrupted_rollover" >:: wrap_tlc test_interrupted_rollover;
  "test_compression_bug" >:: wrap_tlc test_compression_bug;
]
