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
open Tlogcommon
open Tlog_map
open Tlogcollection
open Tlogcollection_test

let section = Logger.Section.main
let node_id = "node_id"
let cluster_id = "cluster_id"

let create_test_tlc ?tlog_max_entries ?tlog_max_size dn =
  let tlx_dir = (dn ^ "_tlx") in
  let compressor = Compression.Snappy in
  Tlc2.make_tlc2
    dn
    ?tlog_max_entries ?tlog_max_size tlx_dir tlx_dir
    ~compressor ~should_fsync:(fun _ _ -> false)
    ~fsync_tlog_dir:false
    ~check_marker:true
    ~cluster_id

let wrap_tlc = Tlogcollection_test.wrap create_test_tlc

let prepare_tlog_scenarios (dn,(factory:factory)) =
  factory ~tlog_max_entries:5 dn node_id >>= fun (tlog_coll:tlog_collection) ->
  let value = Value.create_master_value ~lease_start:0. "me" in
  tlog_coll # log_value  0L value  >>= fun _ ->
  tlog_coll # log_value  1L value  >>= fun _ ->
  tlog_coll # log_value  2L value  >>= fun _ ->
  tlog_coll # log_value  3L value  >>= fun _ ->
  tlog_coll # log_value  4L value  >>= fun _ ->
  tlog_coll # close () >>= fun _ ->
  Lwt.return ()

let test_interrupted_rollover (dn, tlx_dir, factory) =
  let open Tlog_map in
  prepare_tlog_scenarios (dn,factory) >>= fun () ->
  factory ~tlog_max_entries:5 dn node_id >>= fun tlog_coll ->
  let value = Value.create_master_value ~lease_start:0. "me" in
  tlog_coll # log_value 5L value >>= fun _ ->
  tlog_coll # close () >>= fun _ ->
  Tlog_map._get_tlog_names dn tlx_dir >>= fun tlog_names ->
  let n = List.length tlog_names in
  let msg = Printf.sprintf "Number of tlogs incorrect. Expected 2, got %d" n in
  Lwt.return (OUnit.assert_equal ~msg n 2)


let test_validate_at_rollover_boundary (dn, tlx_dir, (factory:Tlogcollection_test.factory)) =
  prepare_tlog_scenarios (dn,factory) >>= fun () ->
  factory ~tlog_max_entries:5 dn node_id >>= fun val_tlog_coll ->
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
  OUnit.assert_equal ~msg lasti 4L;
  Logger.debug_ "2" >>= fun () ->
  factory ~tlog_max_entries:5 dn node_id >>= fun (tlog_coll:tlog_collection) ->
  Logger.debug_ "3" >>= fun () ->
  let value = Value.create_master_value ~lease_start:0. "me" in
  tlog_coll # log_value 5L value >>= fun _ ->
  tlog_coll # log_value 6L value >>= fun _ ->
  tlog_coll # log_value 7L value >>= fun _ ->
  tlog_coll # log_value 8L value >>= fun _ ->
  tlog_coll # log_value 9L value >>= fun _ ->
  tlog_coll # close () >>= fun () ->
  Tlog_map._get_tlog_names dn tlx_dir >>= fun tlog_names ->
  let n = List.length tlog_names in
  let msg = Printf.sprintf "Number of tlogs incorrect. Expected 2, got %d" n in
  Lwt.return (OUnit.assert_equal ~msg n 2)

let test_iterate4 (dn, tlx_dir, (factory:Tlogcollection_test.factory)) =
  Logger.debug_ "test_iterate4" >>= fun () ->
  factory ~tlog_max_entries:100 dn node_id >>= fun (tlc:tlog_collection) ->
  let value = Value.create_client_value [Update.Set("test_iterate4","xxx")] false in
  Tlogcollection_test._log_repeat tlc value 120 >>= fun () ->
  Lwt_unix.sleep 3.0 >>= fun () -> (* TODO: compression should have callback *)
  let extension = Tlog_map.extension Compression.Snappy in

  let fnc = Tlog_map._get_full_path dn tlx_dir ("000" ^ extension) in
  File_system.unlink fnc >>= fun () ->
  (* remove 000.tlog & 000.tlx ; errors? *)
  tlc # get_infimum_i () >>= fun inf ->
  Logger.debug_f_ "inf=%s" (Sn.string_of inf) >>= fun () ->
  OUnit.assert_equal ~printer:Sn.string_of inf (Sn.of_int 100);
  tlc # close () >>= fun () ->
  Logger.debug_f_ "end of test_iterate4" >>= fun () ->
  Lwt.return ()


let test_iterate5 (dn, _tlx_dir, (factory:factory)) =
  factory dn node_id >>= fun (tlc:tlog_collection) ->
  let rec loop (tlc:tlog_collection) i =
    if i = 33
    then Lwt.return ()
    else
      begin
        let sync = false in
        let is = string_of_int i in
        let value = Value.create_client_value [Update.Set("test_iterate_" ^ is ,is)] sync in
        tlc # log_value (Sn.of_int i) value >>= fun _ ->
        begin
          if i mod 3 = 2
          then
            begin
              tlc # close ~wait_for_compression:true () >>= fun () ->
              factory dn node_id
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
  let cb x = Logger.debug_f_ "rotation_point:%i" x in
  tlc # iterate start_i too_far_i f cb
  >>= fun () ->
  Lwt.return ()

let test_iterate6 (dn, _tlx_dir, (factory:factory)) =
  let sync = false in
  factory dn node_id >>= fun (tlc:tlog_collection) ->
  let rec loop i =
    if i = 33
    then Lwt.return ()
    else
      begin
        let is = string_of_int i in
        let sni = Sn.of_int i in
        let value = Value.create_client_value [Update.Set("test_iterate_" ^ is ,is)] sync in
        begin
          if i != 19
          then
            tlc # log_value sni value
          else
            begin
              tlc # log_value sni value >>= fun _ ->
              let value2 = Value.create_client_value [Update.Set("something_else","gotcha")] sync in
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
  let f =
    (fun entry ->
       let i = Entry.i_of entry in
       let v = Entry.v_of entry in
       sum := !sum + (Int64.to_int i);
       Logger.debug_f_ "i=%s : %s" (Sn.string_of i) (Value.value2s v)
       >>= fun () ->
       Lwt.return ())
  in
  let cb _ = Lwt.return_unit in
  tlc # iterate start_i too_far_i f cb
  >>= fun () ->
  tlc # close () >>= fun () ->
  Logger.debug_f_ "sum =%i " !sum >>= fun () ->
  (* OUnit.assert_equal ~printer:string_of_int 19 !sum; *)
  Lwt.return ()

let test_iterate7 (dn, tlx_dir, (factory:factory)) =
  Logger.info_f_ "test_iterate7  %s, %s" dn tlx_dir >>= fun () ->
  let tlog_max_size = 32000 in
  factory ~tlog_max_size dn node_id >>= fun tlc ->
  let make_value size =
    Value.create_client_value [ Update.Set ("", Bytes.create size); ] false
  in
  tlc # log_value 0L (make_value 5) >>= fun _ ->
  tlc # log_value 1L (make_value 5) >>= fun _ ->
  tlc # log_value 2L (make_value tlog_max_size) >>= fun _ ->
  tlc # log_value 3L (make_value tlog_max_size) >>= fun _ ->
  tlc # log_value 4L (make_value 5) >>= fun _ ->
  tlc # log_value 5L (make_value tlog_max_size) >>= fun _ ->
  tlc # log_value 6L (make_value tlog_max_size) >>= fun _ ->
  tlc # log_value 7L (make_value tlog_max_size) >>= fun _ ->

  TlogMap.make dn tlx_dir node_id ~check_marker:false >>= fun (tlog_map,_,_) ->
  Lwt_log.debug_f "%s" (TlogMap.show_map tlog_map) >>= fun () ->
  assert ([ (7L, 4);
            (6L, 3);
            (4L, 2);
            (3L, 1);
            (0L, 0);
          ] =
            List.map
              (fun ({ TlogMap.i; n; is_archive; }) ->
               i, n)
              tlog_map.TlogMap.i_to_tlog_number);

  let next = ref 0L in
  tlc # iterate
      0L 6L
      (fun entry ->
       Lwt_log.debug_f "iter entry: %s" (Tlogcommon.Entry.entry2s entry) >>= fun () ->
       assert (!next = Tlogcommon.Entry.i_of entry);
       next := Int64.succ !next;
       Lwt.return ())
      (fun i -> Lwt.return ())
  >>= fun () ->
  assert (!next = 6L);
  Lwt.return ()

let test_compression_bug (dn, tlx_dir, (factory:factory)) =
  Logger.info_ "test_compression_bug" >>= fun () ->
  let tlog_max_entries = 10 in
  factory ~tlog_max_entries dn node_id >>= fun (tlc:tlog_collection) ->
  let v = Bytes.create (1024 * 1024) in
  let sync = false in
  let n = 12 in
  let rec loop i =
    if i = n then Lwt.return ()
    else
      let key = Printf.sprintf "test_compression_bug_%i" i in
      let value = Value.create_client_value [Update.Set(key, v)] sync in
      let sni = Sn.of_int i in
      tlc # log_value sni value >>= fun _ ->
      loop (i+1)
  in
  tlc # log_value 0L (Value.create_client_value [Update.Set("xxx","XXX")] false)
  >>= fun _total_size ->
  loop 1 >>= fun () ->
  tlc # close ~wait_for_compression:true () >>= fun () ->
  File_system.stat (tlx_dir ^ "/000.tlx") >>= fun stat ->
  OUnit.assert_bool "file should have size >0" (stat.st_size > 0);
  let entries = ref [] in
  factory ~tlog_max_entries dn node_id >>= fun tlc2 ->
  let f entry =
    let i = Entry.i_of entry in
    entries := i :: !entries;
    Logger.debug_f_ "ENTRY: i=%Li" i
  in
  let cb _ = Lwt.return_unit in

  tlc2 # iterate 0L (Sn.of_int n) f cb >>= fun () ->

  OUnit.assert_equal
    ~printer:string_of_int
    ~msg:"tlog_collection has a hole" (n+2) (List.length !entries);
  Lwt.return ()

let test_compression_previous (dn, tlx_dir, (factory:factory)) =
  Logger.info_ "test_compression_previous_tlogs" >>= fun () ->
  let v = Bytes.create (1024 * 1024) in
  factory ~tlog_max_entries:10 dn node_id >>= fun (tlc:tlog_collection) ->
  Logger.info_ "have tlc" >>= fun () ->
  let sync = false in
  let n = 42 in
  let rec loop i =
    if i = n then Lwt.return ()
    else
      let key = Printf.sprintf "test_compression_previous_%i" i in
      let value = Value.create_client_value [Update.Set(key, v)] sync in
      let sni = Sn.of_int i in
      tlc # log_value sni value >>= fun _ ->
      loop (i+1)
  in
  tlc # log_value 0L (Value.create_client_value [Update.Set("xxx","XXX")] false)
  >>= fun _ ->
  loop 1
  >>= fun () ->
  tlc # close ~wait_for_compression:true () >>= fun () ->

  (* mess around : uncompress tlxs to tlogs again, put some temp files in the way *)
  let ext = Tlog_map.extension Compression.Snappy in
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
  factory dn node_id >>= fun tlc2 ->
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

let test_size_based_roll_over1 (dn, tlx_dir, (factory:factory)) =
  Logger.info_ "test_size_based_roll_over" >>= fun () ->
  let v = Bytes.create 1024 in
  factory ~tlog_max_size:32000 dn node_id >>= fun tlc ->
  let n = 100 in
  let rec loop i =
    if i = n
    then Lwt.return_unit
    else
      let key = Printf.sprintf "test_size_based_roll_over_%02i" i in
      let value = Value.create_client_value [Update.Set(key,v)] false in
      let sni = Sn.of_int i in
      tlc # log_value sni value >>= fun _ ->
      loop (i+1)
  in
  loop 0 >>= fun () ->
  tlc # close ~wait_for_compression:true () >>= fun () ->
  File_system.lwt_directory_list dn >>= fun tlog_entries ->
  let printer = string_of_int in
  OUnit.assert_equal 1 (List.length tlog_entries) ~printer;
  File_system.lwt_directory_list tlx_dir >>= fun tlx_entries ->
  OUnit.assert_equal 3 (List.length tlx_entries) ~printer;

  TlogMap.make dn tlx_dir node_id ~check_marker:true >>= fun (second_map,_,_) ->
  OUnit.assert_equal 3 (TlogMap.get_tlog_number second_map) ~printer;
  Lwt.return_unit

let test_get_last_i (dn, tlx_dir, (factory:factory)) =
  let make_value size =
    Value.create_client_value [ Update.Set ("", Bytes.create size); ] false
  in
  let value = make_value 0 in

  factory dn node_id >>= fun tlc ->

  (* this one is a bit special ...
   * IMO the correct return value should be None *)
  tlc # get_last_i () >>= fun last_i ->
  assert (0L = last_i);

  tlc # log_value 0L value >>= fun _ ->
  tlc # get_last_i () >>= fun last_i ->
  assert (0L = last_i);

  tlc # log_value 1L value >>= fun _ ->
  tlc # get_last_i () >>= fun last_i ->
  assert (1L = last_i);

  let ic, oc = Lwt_io.pipe () in

  let _ =
    let tlog_max_size = 32000 in
    Tlogcollection_test.setup factory "test_get_last_i_x2" () >>= fun (dn, tlx_dir, factory) ->
    factory ~tlog_max_size dn node_id >>= fun tlc2 ->
    tlc2 # log_value 0L value >>= fun _ ->
    tlc2 # log_value 1L value >>= fun _ ->
    tlc2 # log_value 2L value >>= fun _ ->
    tlc2 # log_value 3L (make_value tlog_max_size) >>= fun _ ->
    tlc2 # log_value 4L value >>= fun _ ->
    tlc2 # log_value 5L value >>= fun _ ->

    (* did we do what we expected to do? *)
    TlogMap.make dn tlx_dir node_id ~check_marker:false >>= fun (tlog_map,_,_) ->
    assert ([ (4L, 1);
              (0L, 0);
            ] =
              List.map
                (fun ({ TlogMap.i; n; is_archive; }) ->
                 i, n)
                tlog_map.TlogMap.i_to_tlog_number);

    tlc2 # dump_tlog_file
         1
         oc
  in

  Llio.input_string ic >>= fun extension ->
  Sn.input_sn ic >>= fun start_i ->
  assert (start_i = 4L);
  Llio.input_int64 ic >>= fun length ->

  tlc # save_tlog_file
      start_i
      extension
      length
      ic >>= fun () ->
  tlc # get_last_i () >>= fun last_i ->
  Lwt_log.debug_f "got last_i = %Li" last_i >>= fun () ->
  assert (5L = last_i);

  tlc # log_value 6L value >>= fun _ ->
  tlc # get_last_i () >>= fun last_i ->
  Lwt_log.debug_f "got last_i = %Li" last_i >>= fun () ->
  assert (6L = last_i);

  Lwt.return_unit

let first_i_of_compress_race (dn, tlx_dir, (factory : factory)) =
  let tlog_max_size = 1000 in
  factory ~tlog_max_size dn "node_id" >>= fun tlc ->
  let make_value size =
    let open Update in
    Value.create_client_value [ Update.Set ("", Bytes.create size); ] false
  in
  let value = make_value 0 in
  tlc # log_value 0L value >>= fun _ ->
  tlc # log_value 1L value >>= fun _ ->
  tlc # log_value 2L value >>= fun _ ->
  tlc # log_value 3L (make_value tlog_max_size) >>= fun _ ->
  tlc # log_value 4L value >>= fun _ ->
  tlc # log_value 5L value >>= fun _ ->
  (* wait for compress *)
  tlc # close ~wait_for_compression:true () >>= fun () ->

  let tlog = dn ^ "/000.tlog" in
  Lwt_log.debug_f "tlog=%s, tlx_dir=%s" tlog tlx_dir >>= fun () ->
  Tlog_map.first_i_of tlog tlx_dir >>= fun i ->
  assert (i = 0L);
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
                  ("test_iterate7", test_iterate7);
                  ("validate",  Tlogcollection_test.test_validate_normal);
                  ("validate_corrupt", Tlogcollection_test.test_validate_corrupt_1);
                  ("test_rollover_1002", Tlogcollection_test.test_rollover_1002);
                  ("test_rollover_boundary", test_validate_at_rollover_boundary);
                  ("test_interrupted_rollover", test_interrupted_rollover);
                  ("test_compression_bug", test_compression_bug);
                  ("test_compression_previous", test_compression_previous);
                  ("test_size_based_roll_over1", test_size_based_roll_over1);
                  ("test_get_last_i", test_get_last_i);
                  ("first_i_of_compress_race", first_i_of_compress_race);
                ]
