(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

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
             let value = Value.create_client_value updates false in
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
