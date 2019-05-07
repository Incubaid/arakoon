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
let node_id = "node_id"

type factory =
  ?tlog_max_entries:int ->
  ?tlog_max_size:int ->
  string ->
  string ->
  Tlogcollection.tlog_collection Lwt.t

let setup (factory:factory) test_name () =
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

let _make_set_v k v= Value.create_client_value [Update.Set (k,v)] false

let _log_repeat tlc (value:Value.t) n =
  let rec loop i =
    if i = (Sn.of_int n) then Lwt.return ()
    else
      begin
        tlc # log_value i value >>= fun _wr_result ->
        loop (Sn.succ i)
      end
  in loop Sn.start

let test_rollover (dn, tlx_dir, (factory:factory)) =
  Logger.info_f_ "test_rollover %s, %s" dn tlx_dir >>= fun () ->
  factory dn node_id >>= fun (c:tlog_collection) ->
  let value = _make_set_v "x" "y" in
  _log_repeat c value 101 >>= fun () ->
  c # close () >>= fun ()->
  Lwt.return ()


let test_rollover_1002 (dn, tlx_dir, (factory:factory)) =
  Logger.info_f_ "test_rollover_1002 %s, %s" dn tlx_dir >>= fun () ->
  let n = 5 in
  factory ~tlog_max_entries:n dn node_id >>= fun (c:tlog_collection) ->
  let value = _make_set_v "x" "y" in
  let n_updates = 1002 * n + 3 in
  _log_repeat c value n_updates >>= fun () ->
  c # close () >>= fun () ->
  factory dn node_id >>= fun tlc_two ->
  tlc_two # get_last_value (Sn.of_int (n_updates-1)) >>= fun vo ->
  let vos = Log_extra.option2s (Value.value2s ~values:false) vo in
  Logger.info_f_ "last_value = %s" vos >>= fun () ->
  tlc_two # close() >>= fun () ->
  Lwt.return ()

let test_get_value_bug (dn, _tlf_dir, (factory:factory)) =
  Logger.info_ "test_get_value_bug" >>= fun () ->
  factory dn node_id >>= fun (c0:tlog_collection) ->
  let v0 = Value.create_master_value ~lease_start:0. "XXXX" in
  c0 # log_value 0L v0 >>= fun _wr_result ->
  c0 # close () >>= fun () ->
  factory dn node_id >>= fun c1 ->
  (* c1 # validate () >>= fun _ -> *)
  c1 # get_last_value 0L >>= function
    | None -> Llio.lwt_failfmt "get_last_update 0 yields None"
    | Some v -> let () = OUnit.assert_equal v v0 in Lwt.return ()

let test_regexp (_dn, _tlf_dir, _factory) =
  Logger.info_ "test_get_regexp_bug" >>= fun () ->
  let open Compression in
  let tests = ["001.tlog", true;
             "000" ^ Tlog_map.extension Snappy, true;
             "000" ^ Tlog_map.extension Snappy ^ ".part", false;
             "000" ^ Tlog_map.extension Bz2, true;
             "000" ^ Tlog_map.extension Bz2  ^ ".part", false;
            ]
  in
  let test (fn,e) =
    let r = Str.string_match Tlog_map.file_regexp fn 0 in
    OUnit.assert_equal r e
  in
  List.iter test tests;
  Lwt.return ()

let test_restart (dn, _tlf_dir, (factory:factory)) =
  factory dn node_id >>= fun (tlc_one:tlog_collection) ->
  let value = _make_set_v "x" "y" in
  _log_repeat tlc_one value 100 >>= fun () ->
  tlc_one # close () >>= fun () ->
  factory dn node_id >>= fun tlc_two ->
  let _ = tlc_two # get_last_value (Sn.of_int 99) in
  tlc_two # close () >>= fun () ->
  Lwt.return ()


let test_iterate (dn, tlx_dir, (factory: factory)) =
  Logger.info_f_ "test_iterate  %s, %s" dn tlx_dir >>= fun () ->
  factory dn node_id >>= fun  (tlc:tlog_collection) ->
  let value = _make_set_v "xxx" "y" in
  _log_repeat tlc value 323 >>= fun () ->
  let sum = ref 0 in
  let cb _ = Lwt.return_unit in
  let f entry =
       let i = Entry.i_of entry in
       sum := !sum + (Int64.to_int i);
       Logger.debug_f_ "i=%s" (Sn.string_of i) >>= fun () ->
       Lwt.return ()
  in
  tlc # iterate (Sn.of_int 125) (Sn.of_int 304) f cb
  >>= fun () ->
  tlc # close () >>= fun () ->
  Logger.debug_f_ "sum =%i " !sum >>= fun () ->
  OUnit.assert_equal ~printer:string_of_int !sum 38306;
  Lwt.return ()


let test_iterate2 (dn, tlx_dir, (factory:factory)) =
  Logger.info_f_ "test_iterate2  %s, %s" dn tlx_dir >>= fun () ->
  factory dn node_id >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "test_iterate0" "xxx" in
  _log_repeat tlc value 3 >>= fun () ->
  let result = ref [] in
  let f entry =
    let i = Entry.i_of entry in
       result := i :: ! result;
       Logger.debug_f_ "i=%s" (Sn.string_of i) >>= fun () ->
       Lwt.return ()
  in
  let cb _ = Lwt.return_unit in
  tlc # iterate (Sn.of_int 0) (Sn.of_int 1) f cb
    
  >>= fun () ->
  OUnit.assert_equal ~printer:string_of_int 1 (List.length !result);
  tlc # close () >>= fun () ->
  Lwt.return ()


let test_iterate3 (dn, tlx_dir, (factory:factory)) =
  Logger.info_f_ "test_iterate3  %s, %s" dn tlx_dir >>= fun () ->
  factory dn node_id  >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "test_iterate3" "xxx" in
  _log_repeat tlc value 120 >>= fun () ->
  let result = ref [] in
  let f entry =
    let i = Entry.i_of entry in
    Logger.debug_f_ "i=%s" (Sn.string_of i) >>= fun () ->
    let () = result := i :: !result in
    Lwt.return ()
  in
  let cb _ = Lwt.return_unit in
  tlc # iterate (Sn.of_int 99) (Sn.of_int 101) f cb
  >>= fun () ->
  OUnit.assert_equal (List.mem (Sn.of_int 99) !result) true;
  tlc # close () >>= fun () ->
  Lwt.return ()

let test_validate_normal (dn, tlx_dir, (factory:factory)) =
  Logger.info_f_ "test_validate_normal  %s, %s" dn tlx_dir >>= fun () ->
  factory dn node_id >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "XXX" "X" in
  _log_repeat tlc value 123 >>= fun () ->
  tlc # close () >>= fun () ->
  Logger.debug_f_ "reopening %s" dn >>= fun () ->
  factory dn node_id >>= fun (tlc_two:tlog_collection) ->
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

let test_validate_corrupt_1 (dn, tlx_dir, (factory:factory)) =
  let open Tlog_map in
  factory dn node_id >>= fun (tlc:tlog_collection) ->
  let value = _make_set_v "Incompetent" "Politicians" in
  _log_repeat tlc value 42 >>= fun () ->
  tlc # close () >>= fun () ->
  TlogMap.make dn tlx_dir node_id ~check_marker:true >>= fun (tlog_map,_,_) ->
  let fn = TlogMap.get_full_path tlog_map "000.tlog" in
  Lwt_unix.openfile fn [Unix.O_RDWR] 0o640 >>= fun fd ->
  Lwt_unix.lseek fd 666 Unix.SEEK_SET >>= fun _ ->
  Lwt_unix.write fd "\x00\x00\x00\x00\x00\x00" 0 6 >>= fun _ ->
  Lwt_unix.close fd >>= fun () ->
  Logger.info_f_ "corrupted 6 bytes" >>= fun () ->
  Lwt.catch
    (fun () ->
       factory dn node_id >>= fun (tlc_two:tlog_collection) ->
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
  >>= fun () ->
  Lwt.return ()

let wrap (factory:factory) test (name:string) = lwt_bracket (setup factory name) test teardown

let create_test_tlc ?tlog_max_entries ?tlog_max_size dn =
  Mem_tlogcollection.make_mem_tlog_collection
    ?tlog_max_entries ?tlog_max_size
    dn None None ~should_fsync:false ~fsync_tlog_dir:false
    ~cluster_id:""

let wrap_memory name = wrap create_test_tlc name

let suite_mem = "mem_tlogcollection" >::: [
      "rollover" >:: wrap_memory test_rollover "rollover";
    (* "get_value_bug" >:: wrap_memory test_get_value_bug;
        (* assumption that different tlog_collections with the same name have the same state *)
    *)
  ]
