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
open Tlog_map

let section = Logger.Section.main
let compressor = Compression.Snappy
let cluster_id = Some "cluster_id"

module S = (val (Store.make_store_module (module Batched_store.Local_store)))

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

let _make_values tlc start_i n =
  let sync = false in
  let rec loop i =
    if i = n
    then Lwt.return ()
    else
      let a = i mod 10000 in
      let k = Printf.sprintf "sqrt(%i)" a in
      let v = Printf.sprintf "%f" (sqrt (float a)) in
      let update = Update.Set(k, v) in
      let value = Value.create_client_value [update] sync in
      let sni = Sn.of_int i in
      tlc # log_value sni value >>= fun _wr_result ->
      loop (i+1)
  in
  loop start_i

let test_collapse_until (dn, tlx_dir, head_dir) =
  let node_id = "node_name" in
  Logger.debug_f_ "dn=%s, tlf_dir=%s, head_dir=%s" dn tlx_dir head_dir >>= fun () ->
  Tlc2.make_tlc2 ~compressor
                 ~tlog_max_entries:1000
                 dn tlx_dir head_dir
                 ~fsync:false node_id ~fsync_tlog_dir:false
                 ~cluster_id:""
  >>= fun tlc ->
  _make_values tlc 0 1111 >>= fun () ->
  tlc # close () >>= fun () ->
  Lwt_unix.sleep 5.0 >>= fun () -> (* give it time to generate the .tlc *)
  (* now collapse first file into a tc *)
  let storename = "head.db" in
  File_system.unlink storename >>= fun () ->
  let store_methods = (Batched_store.Local_store.copy_store2, storename, 0.0)
  in
  let future_i = Sn.of_int 1001 in
  let cb = fun _s -> Lwt.return () in
  Collapser.collapse_until tlc (module S) store_methods future_i cb None ~cluster_id >>= fun () ->
  (* some verification ? *)

  (* try to do it a second time, it should *)
  let future_i2 = Sn.of_int 1000 in
  _should_fail
    (fun () ->
       Collapser.collapse_until tlc (module S) store_methods future_i2 cb None ~cluster_id)
    "this should fail"
    "great, it indeed refuses to do this"
  >>= fun ()->
  Lwt.return ()




let test_collapse_many (dn, tlx_dir, head_dir) =
  let node_id = "node_name" in
  Logger.debug_f_ "test_collapse_many_regime dn=%s, tlx_dir=%s, head_dir=%s" dn tlx_dir head_dir
  >>= fun () ->
  let make_tlc () = 
    Tlc2.make_tlc2 ~compressor ~tlog_max_entries:100
                 dn tlx_dir head_dir
                 ~fsync:false node_id ~fsync_tlog_dir:false
                 ?cluster_id
  in
  make_tlc () >>= fun tlc ->
  _make_values tlc 0 632 >>= fun () ->
  tlc # close () ~wait_for_compression:true >>= fun () ->
  
  make_tlc () >>= fun tlc ->
  let storename = Filename.concat dn "head.db" in
  let cb n = Logger.debug_f_ "collapsed %03i" n in
  let cb' = fun _ -> Lwt.return () in
  File_system.unlink storename >>= fun () ->
  let store_methods = (Batched_store.Local_store.copy_store2, storename, 0.0) in
  Collapser.collapse_many tlc (module S) store_methods 6 cb' cb None ?cluster_id >>= fun () ->
  Logger.debug_ "collapsed 000" >>= fun () ->
  Collapser.collapse_many tlc (module S) store_methods 4 cb' cb None ?cluster_id >>= fun () ->
  Logger.debug_ "collapsed 001 & 002" >>= fun () ->
  Collapser.collapse_many tlc (module S) store_methods 1 cb' cb None ?cluster_id >>= fun () ->
  Logger.debug_ "collapsed 003 & 004" >>= fun () -> (* ends @ 510 *)
  Lwt.return ()

let test_repeated_collapse (dn,tlx_dir, head_dir) =
  Logger.info_f_ "---------- test_repeated_collapse ----------" >>= fun () ->
  let node_id = "node_name" in
  Logger.debug_f_ "test_collapse_many dn=%s, tlx_dir=%s, head_dir=%s" dn tlx_dir head_dir
  >>= fun () ->
  Tlc2.make_tlc2 ~compressor ~tlog_max_entries:100
                 dn tlx_dir head_dir
                 ~fsync:false node_id ~fsync_tlog_dir:false
                 ?cluster_id
  >>= fun tlc ->
  let head_location= Filename.concat dn "head.db" in
  let count = ref 0 in
  let cb' x =
    Logger.debug_f_ "cb':%i" x >>= fun () ->
    count := x;
    Lwt.return_unit
  in
  let cb  n =
    Logger.debug_f_ "cb:collapsed %03i" n >>= fun () ->
    decr count ;
    Lwt.return_unit
  in
  
  let store_methods = (Batched_store.Local_store.copy_store2, head_location, 0.0) in
  File_system.unlink head_location >>= fun () ->
  let n_entries = 520 in
  _make_values tlc 0 n_entries >>= fun () ->
  let collapse_slowdown = None in
  Collapser.collapse_many tlc (module S) store_methods 1 cb' cb collapse_slowdown ?cluster_id >>= fun () ->
  Logger.debug_f_"post collapse: count=%i" !count >>= fun () ->
  OUnit.assert_equal 0 !count ~printer:string_of_int ~msg:"count should be zero";
  Logger.debug_f_ "adding %i more entries" n_entries >>= fun () ->
  _make_values tlc n_entries (2 * n_entries) >>= fun () ->
  Lwt_unix.sleep 2.0 >>= fun () -> (* just to make debugging less messy *)
  Collapser.collapse_many tlc (module S) store_methods 1 cb' cb collapse_slowdown ?cluster_id >>= fun () ->
  Logger.debug_f_"post collapse: count=%i" !count >>= fun () ->
  OUnit.assert_equal 0 !count ~printer:string_of_int ~msg:"count should be zero";
  tlc # close ~wait_for_compression:true () >>= fun () ->
  Lwt.return()
  
let setup =
  let c = ref 0 in
  (fun () ->
    let () = incr c in
    Logger.info_f_ "Collapser_test.setup_%i" !c >>= fun () ->
    let root =
      let w = try Sys.getenv "WORKSPACE"  with _ -> "" in
      w ^ "/tmp"
    in
    let test_dn  =  Printf.sprintf "%s/collapser_%i"      root !c 
    and _tlx_dir =  Printf.sprintf "%s/collapser_tlx_%i"  root !c 
    and _head_dir = Printf.sprintf "%s/collapser_head_%i" root !c
    in
    let _ = Sys.command (Printf.sprintf "rm -rf '%s'" test_dn) in
    let _ = Sys.command (Printf.sprintf "rm -rf '%s'" _tlx_dir) in 
    let _ = Sys.command (Printf.sprintf "rm -rf '%s'" _head_dir) in
    let _ = Sys.command (Printf.sprintf "mkdir -p '%s'" root) in
    File_system.mkdir test_dn 0o755 >>= fun () ->
    File_system.mkdir _tlx_dir 0o755 >>= fun () ->
    File_system.mkdir _head_dir 0o755 >>= fun () ->
    Lwt.return (test_dn, _tlx_dir, _head_dir))


let teardown (dn, tlx_dir, head_dir) =
  Logger.debug_f_ "teardown %s, %s, %s" dn tlx_dir head_dir


let suite =
  let wrapTest f = Extra.lwt_bracket setup f teardown
  in
  "collapser_test" >:::[
    "collapse_until" >:: wrapTest test_collapse_until;
    "collapse_many" >:: wrapTest test_collapse_many;
    "repeated_collapse" >:: wrapTest test_repeated_collapse;
  ]
