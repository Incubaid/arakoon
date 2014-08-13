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
open Unix
open Lwt
open Update
open Tlogcollection
open Tlogcommon

let section = Logger.Section.main

let create_test_tlc dn =
  let tlx_dir = (dn ^ "_tlx") in
  let compressor = Compression.Snappy in
  Tlc2.make_tlc2 dn tlx_dir tlx_dir ~compressor ~fsync:false ~fsync_tlog_dir:false
let wrap_tlc = Tlogcollection_test.wrap create_test_tlc

let prepare_tlog_scenarios (dn,factory) =
  let old_tlog_entries_value = !Tlogcommon.tlogEntriesPerFile in
  Tlogcommon.tlogEntriesPerFile := 5 ;
  factory dn "node_name" >>= fun (tlog_coll:tlog_collection) ->
  let value i = Value.create_master_value tlog_coll i ~lease_start:0. "me" in
  tlog_coll # log_value  0L (value 0L)  >>= fun () ->
  tlog_coll # log_value  1L (value 1L)  >>= fun () ->
  tlog_coll # log_value  2L (value 2L)  >>= fun () ->
  tlog_coll # log_value  3L (value 3L)  >>= fun () ->
  tlog_coll # log_value  4L (value 4L)  >>= fun () ->
  tlog_coll # close () >>= fun _ ->
  Lwt.return old_tlog_entries_value

let test_interrupted_rollover (dn, tlx_dir, factory) =
  prepare_tlog_scenarios (dn,factory) >>= fun old_tlog_entries_value ->
  (*let fn = Tlc2.get_full_path dn tlx_dir "001.tlog" in
    Unix.unlink fn; *)
  factory dn "node_name" >>= fun tlog_coll ->
  let value = Value.create_master_value tlog_coll 5L ~lease_start:0. "me" in
  tlog_coll # log_value 5L value >>= fun () ->
  tlog_coll # close () >>= fun _ ->
  Tlc2.get_tlog_names dn tlx_dir >>= fun tlog_names ->
  let n = List.length tlog_names in
  Tlogcommon.tlogEntriesPerFile := old_tlog_entries_value;
  let msg = Printf.sprintf "Number of tlogs incorrect. Expected 2, got %d" n in
  Lwt.return (OUnit.assert_equal ~msg n 2)


let test_validate_at_rollover_boundary (dn, tlx_dir, factory) =
  prepare_tlog_scenarios (dn,factory) >>= fun old_tlog_entries_value ->
  factory dn "node_name" >>= fun val_tlog_coll ->
  Logger.debug_ "1" >>= fun () ->
  val_tlog_coll # validate_last_tlog () >>= fun (_validity, lasteo, _index) ->
  let lasti, lasti_str =
    begin
      match lasteo with
        | None -> Sn.start, "None"
        | Some e -> let i = Entry.i_of e in i, Sn.string_of i
    end
  in
  val_tlog_coll # close () >>= fun () ->
  let msg = Printf.sprintf "Values of is are different 4 <> %s" lasti_str in
  begin
    if lasti <> 4L
    then Tlogcommon.tlogEntriesPerFile := old_tlog_entries_value
  end;
  OUnit.assert_equal ~msg lasti 4L;
  factory dn "node_name" >>= fun (tlog_coll:tlog_collection) ->
  let value i = Value.create_master_value tlog_coll i ~lease_start:0. "me" in
  tlog_coll # log_value 5L (value 5L) >>= fun _ ->
  tlog_coll # log_value 6L (value 6L) >>= fun _ ->
  tlog_coll # log_value 7L (value 7L) >>= fun _ ->
  tlog_coll # log_value 8L (value 8L) >>= fun _ ->
  tlog_coll # log_value 9L (value 9L) >>= fun _ ->
  Tlc2.get_tlog_names dn tlx_dir >>= fun tlog_names ->
  let n = List.length tlog_names in
  Tlogcommon.tlogEntriesPerFile := old_tlog_entries_value;
  let msg = Printf.sprintf "Number of tlogs incorrect. Expected 2, got %d" n in
  Lwt.return (OUnit.assert_equal ~msg n 2)

let test_iterate4 (dn, tlx_dir, factory) =
  Logger.debug_ "test_iterate4" >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  let value = Value.create_client_value_nocheck [Update.Set("test_iterate4","xxx")] false in
  Tlogcollection_test._log_repeat tlc value 120 >>= fun () ->
  Lwt_unix.sleep 3.0 >>= fun () -> (* TODO: compression should have callback *)
  let extension = Tlc2.extension Compression.Snappy in
  let fnc = Tlc2.get_full_path dn tlx_dir ("000" ^ extension) in
  File_system.unlink fnc >>= fun () ->
  (* remove 000.tlog & 000.tlx ; errors? *)
  tlc # get_infimum_i () >>= fun inf ->
  Logger.debug_f_ "inf=%s" (Sn.string_of inf) >>= fun () ->
  OUnit.assert_equal ~printer:Sn.string_of inf (Sn.of_int 100);
  tlc # close () >>= fun () ->
  Logger.debug_f_ "end of test_iterate4" >>= fun () ->
  Lwt.return ()


let test_iterate5 (dn, _tlx_dir, factory) =
  let () = Tlogcommon.tlogEntriesPerFile := 10 in
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  let rec loop (tlc:tlog_collection) i =
    if i = 33
    then Lwt.return ()
    else
      begin
        let sync = false in
        let is = string_of_int i in
        let sni = Sn.of_int i in
        let value = Value.create_client_value tlc sni [Update.Set("test_iterate_" ^ is ,is)] sync in
        tlc # log_value sni value >>= fun _ ->
        begin
          if i mod 3 = 2
          then
            begin
              tlc # close ~wait_for_compression:true () >>= fun () ->
              factory dn "node_name"
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
  let f entry =
    let i = Entry.i_of entry in
    let v = Entry.v_of entry in
    Logger.debug_f_ "test_iterate5: %s %s" (Sn.string_of i)
      (Value.value2s v)
  in
  tlc # iterate start_i too_far_i f >>= fun () ->
  Lwt.return ()

let test_iterate6 (dn, _tlx_dir, factory) =
  let () = Tlogcommon.tlogEntriesPerFile := 10 in
  let sync = false in
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  let rec loop i =
    if i = 33
    then Lwt.return ()
    else
      begin
        let is = string_of_int i in
        let sni = Sn.of_int i in
        let value = Value.create_client_value tlc sni [Update.Set("test_iterate_" ^ is ,is)] sync in
        begin
          if i != 19
          then
            tlc # log_value sni value
          else
            begin
              tlc # log_value sni value >>= fun _ ->
              let value2 = Value.create_client_value tlc sni [Update.Set("something_else","gotcha")] sync in
              tlc # log_value  sni value2
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
    (fun entry ->
       let i = Entry.i_of entry in
       let v = Entry.v_of entry in
       sum := !sum + (Int64.to_int i);
       Logger.debug_f_ "i=%s : %s" (Sn.string_of i) (Value.value2s v)
       >>= fun () ->
       Lwt.return ())
  >>= fun () ->
  tlc # close () >>= fun () ->
  Logger.debug_f_ "sum =%i " !sum >>= fun () ->
  (* OUnit.assert_equal ~printer:string_of_int 19 !sum; *)
  Lwt.return ()


let test_compression_bug (dn, tlx_dir, factory) =
  Logger.info_ "test_compression_bug" >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 10 in
  let v = String.create (1024 * 1024) in
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  Logger.info_ "have tls" >>= fun () ->
  let sync = false in
  let n = 12 in
  let rec loop i =
    if i = n then Lwt.return ()
    else
      let key = Printf.sprintf "test_compression_bug_%i" i in
      let sni = Sn.of_int i in
      let value = Value.create_client_value tlc sni [Update.Set(key, v)] sync in
      tlc # log_value sni value >>= fun () ->
      loop (i+1)
  in
  tlc # log_value 0L (Value.create_client_value tlc 0L [Update.Set("xxx","XXX")] false) >>= fun () ->
  loop 1 >>= fun () ->
  tlc # close ~wait_for_compression:true () >>= fun () ->
  File_system.stat (tlx_dir ^ "/000.tlx") >>= fun stat ->
  OUnit.assert_bool "file should have size >0" (stat.st_size > 0);
  let entries = ref [] in
  factory dn "node_name" >>= fun tlc2 ->
  tlc2 # iterate 0L (Sn.of_int n)
    (fun entry ->
       let i = Entry.i_of entry in
       entries := i :: !entries;
       Logger.debug_f_ "ENTRY: i=%Li" i)
  >>= fun () ->
  OUnit.assert_equal
    ~printer:string_of_int
    ~msg:"tls has a hole" (n+2) (List.length !entries);
  Lwt.return ()

let test_compression_previous (dn, tlx_dir, factory) =
  Logger.info_ "test_compression_previous_tlogs" >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 10 in
  let v = String.create (1024 * 1024) in
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  Logger.info_ "have tlc" >>= fun () ->
  let sync = false in
  let n = 42 in
  let rec loop i =
    if i = n then Lwt.return ()
    else
      let key = Printf.sprintf "test_compression_bug_%i" i in
      let sni = Sn.of_int i in
      let value = Value.create_client_value tlc sni [Update.Set(key, v)] sync in
      tlc # log_value sni value >>= fun () ->
      loop (i+1)
  in
  tlc # log_value 0L (Value.create_client_value tlc 0L [Update.Set("xxx","XXX")] false) >>= fun () ->
  loop 1 >>= fun () ->
  tlc # close ~wait_for_compression:true () >>= fun () ->

  (* mess around : uncompress tlxs to tlogs again, put some temp files in the way *)
  let ext = Tlc2.extension Compression.Snappy in
  let uncompress tlx =
    let tlxpath =  (tlx_dir ^ "/" ^ tlx ^ ext) in
    Compression.uncompress_tlog tlxpath (dn ^ "/" ^ tlx ^ ".tlog") >>= fun () ->
    File_system.unlink tlxpath
  in
  let compresseds = ["000"; "001"; "002"; "003"] in
  Lwt_list.iter_s uncompress compresseds >>= fun () ->
  File_system.lwt_directory_list dn >>= fun tlog_entries ->
  assert ((List.length tlog_entries) = 5);

  let touch fn =
    let fd = Unix.openfile fn [Unix.O_CREAT] 0o644 in Unix.close fd in
  touch (tlx_dir ^ "/000" ^ ext ^ ".part");

  (* open tlog again *)
  factory dn "node_name" >>= fun tlc2 ->
  (* give tlogcollection a little time to create the compression jobs *)
  Lwt_unix.sleep 1.0 >>= fun () ->
  tlc2 # close ~wait_for_compression:true () >>= fun () ->

  let verify_exists tlx =
    File_system.exists (tlx_dir ^ "/" ^ tlx ^ ext) >>= fun exists ->
    assert exists;
    Lwt.return () in
  Lwt_list.iter_s verify_exists compresseds >>= fun () ->
  File_system.lwt_directory_list dn >>= fun tlog_entries ->
  assert ((List.length tlog_entries) = 1);
  Lwt.return ()



let make_test_tlc (x, y) = x >:: wrap_tlc y x

let suite = "tlc2" >:::
              List.map make_test_tlc
                [
                  ("regexp", Tlogcollection_test.test_regexp);
                  ("rollover", Tlogcollection_test.test_rollover);
                  ("get_value_bug", Tlogcollection_test.test_get_value_bug);
                  ("test_restart", Tlogcollection_test.test_restart);
                  ("test_iterate", Tlogcollection_test.test_iterate);
                  ("test_iterate2", Tlogcollection_test.test_iterate2);
                  ("test_iterate3", Tlogcollection_test.test_iterate3);
                  ("test_iterate4", test_iterate4);
                  ("test_iterate5", test_iterate5);
                  ("test_iterate6", test_iterate6);
                  ("validate",  Tlogcollection_test.test_validate_normal);
                  ("validate_corrupt", Tlogcollection_test.test_validate_corrupt_1);
                  ("test_checksum", Tlogcollection_test.test_checksum);
                  ("test_rollover_1002", Tlogcollection_test.test_rollover_1002);
                  ("test_rollover_boundary", test_validate_at_rollover_boundary);
                  ("test_interrupted_rollover", test_interrupted_rollover);
                  ("test_compression_bug", test_compression_bug);
                  ("test_compression_previous", test_compression_previous);
                ]
