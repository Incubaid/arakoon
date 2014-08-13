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
open Extra
open Lwt
open Update
open Tlogcollection
open Tlogcommon

let section = Logger.Section.main

let setup factory test_name () =
  let dn = Printf.sprintf "/tmp/%s" test_name in
  let tlf_dir = Printf.sprintf "%s_tlx" dn in
  Logger.info_f_ "setup %s" dn >>= fun () ->
  let make_dir dir =
    File_system.mkdir dir 0o755 >>= fun () ->
    Logger.info_f_ "created %s" dir in
  let prepare_dir dir =
    File_system.exists dir >>= (function
        | true ->
          begin
            Logger.info_f_ "%s exists cleaning" dir >>= fun () ->
            let cmd = Lwt_process.shell (Printf.sprintf "rm -rf %s" dir) in
            Lwt_process.exec cmd
            >>= fun status ->
            begin
              match status with
                | Unix.WEXITED rc when rc = 0 -> make_dir dir
                | Unix.WEXITED rc             -> Llio.lwt_failfmt "rm -rf '%s' gave rc %i" dir rc
                | Unix.WSIGNALED _ | Unix.WSTOPPED _  -> Llio.lwt_failfmt "rm -rf '%s' failed" dir
            end

          end
        | false -> make_dir dir
      ) in
  prepare_dir dn >>= fun () ->
  prepare_dir tlf_dir >>= fun () ->
  Lwt.return (dn, tlf_dir, factory)



let teardown (dn, tlf_dir, _factory) =
  Logger.info_f_ "teardown %s,%s" dn tlf_dir

let _make_set_v k v= Value.create_client_value_nocheck [Update.Set (k,v)] false

let _log_repeat tlc ((_, c):Value.t) n =
  let rec loop i =
    if i = (Sn.of_int n) then Lwt.return ()
    else
      begin
        let value = Value.create_value tlc i c in
        tlc # log_value i value >>= fun _wr_result ->
        loop (Sn.succ i)
      end
  in loop Sn.start

let test_rollover (dn, tlf_dir, factory) =
  Logger.info_f_ "test_rollover %s, %s" dn tlf_dir >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 5 in
  factory dn "node_name" >>= fun (c:tlog_collection) ->
  let value = _make_set_v "x" "y" in
  _log_repeat c value 101 >>= fun () ->
  c # close () >>= fun ()->
  Lwt.return ()


let test_rollover_1002 (dn, tlf_dir, factory) =
  Logger.info_f_ "test_rollover_1002 %s, %s" dn tlf_dir >>= fun () ->
  let n = 5 in
  let () = Tlogcommon.tlogEntriesPerFile := n in
  factory dn "node_name" >>= fun (c:tlog_collection) ->
  let value = _make_set_v "x" "y" in
  let n_updates = 1002 * n + 3 in
  _log_repeat c value n_updates >>= fun () ->
  c # close () >>= fun () ->
  factory dn "node_name" >>= fun tlc_two ->
  let vo = tlc_two # get_last_value (Sn.of_int (n_updates-1)) in
  let vos = Log_extra.option2s Value.value2s vo in
  Logger.info_f_ "last_value = %s" vos >>= fun () ->
  tlc_two # close() >>= fun () ->
  Lwt.return ()

let test_get_value_bug (dn, _tlf_dir, factory) =
  Logger.info_ "test_get_value_bug" >>= fun () ->
  factory dn "node_name" >>= fun (c0:tlog_collection) ->
  let v0 = Value.create_master_value c0 0L ~lease_start:0. "XXXX" in
  c0 # log_value 0L v0 >>= fun _wr_result ->
  c0 # close () >>= fun () ->
  factory dn "node_name" >>= fun c1 ->
  (* c1 # validate () >>= fun _ -> *)
  match c1 # get_last_value 0L with
    | None -> Llio.lwt_failfmt "get_last_update 0 yields None"
    | Some v -> let () = OUnit.assert_equal v v0 in Lwt.return ()

let test_regexp (_dn, _tlf_dir, _factory) =
  Logger.info_ "test_get_regexp_bug" >>= fun () ->
  let open Compression in
  let tests = ["001.tlog", true;
             "000" ^ Tlc2.extension Snappy, true;
             "000" ^ Tlc2.extension Snappy ^ ".part", false;
             "000" ^ Tlc2.extension Bz2, true;
             "000" ^ Tlc2.extension Bz2  ^ ".part", false;
            ]
  in
  let test (fn,e) =
    let r = Str.string_match Tlc2.file_regexp fn 0 in
    OUnit.assert_equal r e
  in
  List.iter test tests;
  Lwt.return ()

let test_restart (dn, _tlf_dir, factory) =
  factory dn "node_name" >>= fun (tlc_one:tlog_collection) ->
  let value = _make_set_v "x" "y" in
  _log_repeat tlc_one value 100 >>= fun () ->
  tlc_one # close () >>= fun () ->
  factory dn "node_name" >>= fun tlc_two ->
  let _ = tlc_two # get_last_value (Sn.of_int 99) in
  tlc_two # close () >>= fun () ->
  Lwt.return ()


let test_iterate (dn, tlf_dir, factory) =
  Logger.info_f_ "test_iterate  %s, %s" dn tlf_dir >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn "node_name" >>= fun  (tlc:tlog_collection) ->
  let value = _make_set_v "xxx" "y" in
  _log_repeat tlc value 323 >>= fun () ->
  let sum = ref 0 in
  tlc # iterate (Sn.of_int 125) (Sn.of_int 304)
    (fun entry ->
       let i = Entry.i_of entry in
       sum := !sum + (Int64.to_int i);
       Logger.debug_f_ "i=%s" (Sn.string_of i) >>= fun () ->
       Lwt.return ())
  >>= fun () ->
  tlc # close () >>= fun () ->
  Logger.debug_f_ "sum =%i " !sum >>= fun () ->
  OUnit.assert_equal ~printer:string_of_int !sum 38306;
  Lwt.return ()


let test_iterate2 (dn, tlf_dir, factory) =
  Logger.info_f_ "test_iterate2  %s, %s" dn tlf_dir >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "test_iterate0" "xxx" in
  _log_repeat tlc value 3 >>= fun () ->
  let result = ref [] in
  tlc # iterate (Sn.of_int 0) (Sn.of_int 1)
    (fun entry ->
       let i = Entry.i_of entry in
       result := i :: ! result;
       Logger.debug_f_ "i=%s" (Sn.string_of i) >>= fun () ->
       Lwt.return ())
  >>= fun () ->
  OUnit.assert_equal ~printer:string_of_int 1 (List.length !result);
  tlc # close () >>= fun () ->
  Lwt.return ()


let test_iterate3 (dn, tlf_dir, factory) =
  Logger.info_f_ "test_iterate3  %s, %s" dn tlf_dir >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "test_iterate3" "xxx" in
  _log_repeat tlc value 120 >>= fun () ->
  let result = ref [] in
  tlc # iterate (Sn.of_int 99) (Sn.of_int 101)
    (fun entry ->
       let i = Entry.i_of entry in
       Logger.debug_f_ "i=%s" (Sn.string_of i) >>= fun () ->
       let () = result := i :: !result in
       Lwt.return ()
    )
  >>= fun () ->
  OUnit.assert_equal (List.mem (Sn.of_int 99) !result) true;
  tlc # close () >>= fun () ->
  Lwt.return ()

let test_validate_normal (dn, tlf_dir, factory) =
  Logger.info_f_ "test_validate_normal  %s, %s" dn tlf_dir >>= fun () ->
  let () = Tlogcommon.tlogEntriesPerFile:= 100 in
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "XXX" "X" in
  _log_repeat tlc value 123 >>= fun () ->
  tlc # close () >>= fun () ->
  Logger.debug_f_ "reopening %s" dn >>= fun () ->
  factory dn "node_name" >>= fun (tlc_two:tlog_collection) ->
  tlc_two # validate_last_tlog () >>= fun result ->
  let _validity, eo, _ = result in
  let wsn = Sn.of_int 122 in
  let wanted = (Some wsn) in
  let io = match eo with None -> None | Some e -> Some (Entry.i_of e) in
  let tos x= Log_extra.option2s Sn.string_of x in
  Logger.info_f_ "wanted:%s, got:%s" (tos wanted) (tos io)
  >>= fun() ->
  OUnit.assert_equal io wanted ;
  Lwt.return ()

let test_validate_corrupt_1 (dn, tlf_dir, factory) =
  let () = Tlogcommon.tlogEntriesPerFile:= 100 in
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "Incompetent" "Politicians" in
  _log_repeat tlc value 42 >>= fun () ->
  tlc # close () >>= fun () ->
  let fn = Tlc2.get_full_path dn tlf_dir "000.tlog" in
  Lwt_unix.openfile fn [Unix.O_RDWR] 0o640 >>= fun fd ->
  Lwt_unix.lseek fd 666 Unix.SEEK_SET >>= fun _ ->
  Lwt_unix.write fd "\x00\x00\x00\x00\x00\x00" 0 6 >>= fun _ ->
  Lwt_unix.close fd >>= fun () ->
  Logger.info_f_ "corrupted 6 bytes" >>= fun () ->
  Lwt.catch
    (fun () ->
       factory dn "node_name" >>= fun (tlc_two:tlog_collection) ->
       tlc_two # validate_last_tlog () >>= fun _ ->
       tlc_two # close () >>= fun () ->
       OUnit.assert_bool "this tlog should not be valid" false;
       Lwt.return ()
    )
    (function
      | TLogCheckSumError _pos
      | TLogUnexpectedEndOfFile _pos ->
        Lwt.return ()
      | exn ->
        let () = ignore exn in
        let msg = Printf.sprintf "it threw the wrong exception %s" "?" in
        OUnit.assert_bool msg false;
        Lwt.return ()
    )

let test_checksum (dn, tlf_dir, factory) =
  Logger.info_f_ "test_checksum  %s, %s" dn tlf_dir >>= fun () ->
  factory dn "node_name" >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "XXX" "X" in
  _log_repeat tlc value 10 >>= fun () ->
  let value = Value.create_client_value tlc 9L [Update.Set ("XXX", "XX")] false in
  Lwt.catch
    (fun () ->
       tlc # log_value 10L value >>= fun () ->
       OUnit.assert_bool "the checksum should be wrong" false;
       Lwt.return ()
    )
    (function
      | Value.ValueCheckSumError _ -> Lwt.return ()
      | exn ->
        let () = ignore exn in
        let msg = Printf.sprintf "it threw the wrong exception %s" "?" in
        OUnit.assert_bool msg false;
        Lwt.return ()
    )

let wrap factory test (name:string) = lwt_bracket (setup factory name) test teardown

let create_test_tlc dn = Mem_tlogcollection.make_mem_tlog_collection dn None None ~fsync:false ~fsync_tlog_dir:false

let wrap_memory name = wrap create_test_tlc name

let suite_mem = "mem_tlogcollection" >::: [
    "rollover" >:: wrap_memory test_rollover "rollover";
    "checksum" >:: wrap_memory test_checksum "checksum";
    (* "get_value_bug" >:: wrap_memory test_get_value_bug;
        (* assumption that different tlog_collections with the same name have the same state *)
    *)
  ]
