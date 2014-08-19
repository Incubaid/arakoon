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



open Compression
open Lwt
open Extra
open OUnit
open Tlogwriter
open Update
let section = Logger.Section.main

let test_compress_file which () =
  let archive_name, compressor, _tlog_name = which in
  Logger.info Logger.Section.main "test_compress_file" >>= fun () ->
  let tlog_name = "/tmp/test_compress_file.tlog" in
  Lwt_io.with_file tlog_name ~mode:Lwt_io.output
    (fun oc ->
       let writer = new tlogWriter oc 0L in
       let rec loop i =
         if i = 100000L
         then Lwt.return ()
         else
           begin
             let v = Printf.sprintf "<xml_bla>value%Li</xml_bla>" i in
             let updates = [Update.Set ("x", v)] in
             let value = Value.create_client_value_nocheck updates false in
             writer # log_value i value >>= fun _ ->
             loop (Int64.succ i)
           end
       in loop 0L
    ) >>= fun () ->
  let arch_name = archive_name tlog_name in
  compress_tlog ~cancel:(ref false) tlog_name arch_name compressor
  >>= fun () ->
  let tlog2_name = _tlog_name arch_name in
  Logger.debug_f_ "comparing %s with %s" tlog2_name tlog_name >>= fun ()->
  OUnit.assert_equal ~printer:(fun s -> s) tlog2_name tlog_name;
  let tlog_name' = (tlog_name ^".restored") in
  uncompress_tlog arch_name tlog_name'
  >>= fun () ->
  let md5 = Digest.file tlog_name in
  let md5' = Digest.file tlog_name' in
  OUnit.assert_equal md5 md5';
  Lwt.return()

let w= lwt_test_wrap

let snappy =
  let archive_name x = x ^ ".tlx"
  and compressor = Compression.Snappy
  and tlog_name a = Filename.chop_extension a
  in
  (archive_name, compressor, tlog_name)

let bz2 =
  let archive_name x = x ^ ".tlf"
  and compressor = Compression.Bz2
  and tlog_name a = Filename.chop_extension a
  in
  (archive_name, compressor, tlog_name)

let suite = "compression" >:::[
    "file_bz2" >:: w (test_compress_file bz2);
    "file_snappy" >::w (test_compress_file snappy);
  ]
