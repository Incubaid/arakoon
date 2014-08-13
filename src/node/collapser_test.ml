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



open Lwt
open OUnit
open Update

let section = Logger.Section.main
let compressor = Compression.Snappy

module S = (val (Store.make_store_module (module Batched_store.Local_store)))


let test_dn = "/tmp/collapser"
let _tlf_dir = "/tmp/collapser_tlf"
let _head_dir = "/tmp/collapser_head"


let _should_fail x error_msg success_msg =
  Lwt.catch
    (fun ()  ->
       x () >>= fun () ->
       Logger.debug_ "should fail...doesn't" >>= fun () ->
       Lwt.return true)

    (fun exn -> Logger.debug_ ~exn success_msg >>= fun () -> Lwt.return false)
  >>= fun bad ->
  if bad then Lwt.fail (Failure error_msg)
  else Lwt.return ()

let _make_values tlc n =
  let sync = false in
  let rec loop i =
    if i = n
    then Lwt.return ()
    else
      let a = i mod 10000 in
      let k = Printf.sprintf "sqrt(%i)" a in
      let v = Printf.sprintf "%f" (sqrt (float a)) in
      let update = Update.Set(k, v) in
      let sni = Sn.of_int i in
      let value = Value.create_client_value tlc sni [update] sync in
      tlc # log_value sni value >>= fun _wr_result ->
      loop (i+1)
  in
  loop 0

let test_collapse_until (dn, tlf_dir, head_dir) =
  let () = Tlogcommon.tlogEntriesPerFile := 1000 in
  Logger.debug_f_ "dn=%s, tlf_dir=%s, head_dir=%s" dn tlf_dir head_dir >>= fun () ->
  Tlc2.make_tlc2 ~compressor dn tlf_dir head_dir
                 ~fsync:false "node_name" ~fsync_tlog_dir:false >>= fun tlc ->
  _make_values tlc 1111 >>= fun () ->
  tlc # close () >>= fun () ->
  Lwt_unix.sleep 5.0 >>= fun () -> (* give it time to generate the .tlc *)
  (* now collapse first file into a tc *)
  let storename = Filename.concat test_dn "head.db" in
  File_system.unlink storename >>= fun () ->
  let store_methods = (Batched_store.Local_store.copy_store2, storename, 0.0)
  in
  let future_i = Sn.of_int 1001 in
  let cb = fun _s -> Lwt.return () in
  Collapser.collapse_until tlc (module S) store_methods future_i cb None >>= fun () ->
  (* some verification ? *)

  (* try to do it a second time, it should *)
  let future_i2 = Sn.of_int 1000 in
  _should_fail
    (fun () ->
       Collapser.collapse_until tlc (module S) store_methods future_i2 cb None)
    "this should fail"
    "great, it indeed refuses to do this"
  >>= fun ()->
  Lwt.return ()


let test_collapse_many (dn, tlf_dir, head_dir) =
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  Logger.debug_f_ "test_collapse_many_regime dn=%s, tlf_dir=%s, head_dir=%s" dn tlf_dir head_dir >>= fun () ->
  Tlc2.make_tlc2 ~compressor dn tlf_dir head_dir
                 ~fsync:false "node_name" ~fsync_tlog_dir:false >>= fun tlc ->
  _make_values tlc 632 >>= fun () ->
  tlc # close () >>= fun () ->
  Lwt_unix.sleep 5.0 >>= fun () -> (* compression finished ? *)
  let storename = Filename.concat test_dn "head.db" in
  let cb fn = Logger.debug_f_ "collapsed %s" (Sn.string_of fn) in
  let cb' = fun _n -> Lwt.return () in
  File_system.unlink storename >>= fun () ->
  let store_methods = (Batched_store.Local_store.copy_store2, storename, 0.0) in
  Collapser.collapse_many tlc (module S) store_methods 5 cb' cb None >>= fun () ->
  Logger.debug_ "collapsed 000" >>= fun () ->
  Collapser.collapse_many tlc (module S) store_methods 3 cb' cb None >>= fun () ->
  Logger.debug_ "collapsed 001 & 002" >>= fun () ->
  Collapser.collapse_many tlc (module S) store_methods 1 cb' cb None >>= fun () ->
  Logger.debug_ "collapsed 003 & 004" >>= fun () -> (* ends @ 510 *)
  Lwt.return ()


let setup () =
  Logger.info_ "Collapser_test.setup" >>= fun () ->

  let _ = Sys.command (Printf.sprintf "rm -rf '%s'" test_dn) in
  let _ = Sys.command (Printf.sprintf "rm -rf '%s'" _tlf_dir) in
  let _ = Sys.command (Printf.sprintf "rm -rf '%s'" _head_dir) in
  File_system.mkdir test_dn 0o755 >>= fun () ->
  File_system.mkdir _tlf_dir 0o755 >>= fun () ->
  File_system.mkdir _head_dir 0o755 >>= fun () ->
  Lwt.return (test_dn, _tlf_dir, _head_dir)


let teardown (dn, tlf_dir, head_dir) =
  Logger.debug_f_ "teardown %s, %s, %s" dn tlf_dir head_dir


let suite =
  let wrapTest f = Extra.lwt_bracket setup f teardown
  in
  "collapser_test" >:::[
    "collapse_until" >:: wrapTest test_collapse_until;
    "collapse_many" >:: wrapTest test_collapse_many;
  ]
