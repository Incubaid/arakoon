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


open Lwt
open Tlogwriter
open Tlogreader
open Update
open Tlogcommon
open Tlogutil
open Otc
open OUnit
open Extra

let fileName = "/tmp/log"

let setupTlogWriter () =
  Lwt_io.open_file ~flags:[Unix.O_CREAT; Unix.O_WRONLY; Unix.O_TRUNC ]
    ~perm:0o644 ~mode:Lwt_io.output fileName >>= fun oc ->
  Lwt.return (new tlogWriter oc Sn.start)

let setupTlogReader () =
  Lwt_io.open_file ~mode:Lwt_io.input fileName >>= fun ic ->
  Lwt.return (new tlogReader ic)

let compareDigests calculatedMd5Hex expectedMd5Hex =
  if calculatedMd5Hex <>  expectedMd5Hex
  then
    Llio.lwt_failfmt "Digest different than expected '%s' != '%s' "
      calculatedMd5Hex expectedMd5Hex
  else Lwt.return()

let validateString str expectedMd5Hex =
  let calculatedMd5 = Digest.string str in
  let calculatedMd5Hex = Digest.to_hex calculatedMd5 in
  compareDigests calculatedMd5Hex expectedMd5Hex

let validateFile fileName expectedMd5Hex =
  let calculatedMd5 = Digest.file fileName in
  let calculatedMd5Hex = Digest.to_hex calculatedMd5 in
  compareDigests calculatedMd5Hex expectedMd5Hex

let validateTlogDump entryCount expectedMd5Hex =
  let readableBuf = String.make (128*1024) '\x00' in
  let oc = Lwt_io.of_string ~mode:Lwt_io.output readableBuf in
  Lwt_io.with_file ~mode:Lwt_io.input fileName
    (fun ic -> printLastEntries oc ic entryCount) >>= fun() ->
  validateString readableBuf expectedMd5Hex


let cleanupTlog () =
  Unix.unlink fileName ;
  Lwt.return()

let cleanupFiles () =
  cleanupTlog () >>= fun() ->
  Lwt.return ()

let test_log_set (tlogWriter,tlogReader)  =
  let u = Update.Set("key","value") in
  tlogWriter # log_update 1L u >>= fun wrResult ->
  validateTlogDump (-1) "0dfbe8aa4c20b52e1b8bf3cb6cbdf193"

let test_log_delete (tlogWriter,tlogReader)  =
  let u = Update.Delete "key" in
  tlogWriter # log_update 1L u >>= fun wrResult ->
  validateTlogDump (-1) "0dfbe8aa4c20b52e1b8bf3cb6cbdf193"

let test_validate_tlog (tlogWriter,tlogReader)  =
  let rec loop = function
    | 250L -> Lwt.return ()
    | i ->
      let key = Printf.sprintf "key_%s" (Sn.string_of i) in
      let value = Printf.sprintf "value_%s" (Sn.string_of i) in
      let i0 = Sn.mul 2L i in
      let i1 = Sn.succ i0 in
      let u0 = Update.Delete key in
      let u1 = Update.Set(key,value) in
      tlogWriter # log_update i0 u0 >>= fun wrResult ->
      tlogWriter # log_update i1 u1 >>= fun wrResult ->
      loop (Sn.succ i)
  in loop 0L >>= fun() ->
  tlogReader # validateTlog () >>= fun _ -> Lwt.return ()


let test_validate_empty (tlogWriter, tlogReader) =
  tlogReader # validateTlog () >>= fun (v,previous) ->
  OUnit.assert_equal previous None ;
  Lwt.return ()


let setup () =
  Lwt_log.info "Tlog_test.setup" >>= fun () ->
  setupTlogWriter () >>= fun tlogWriter ->
  setupTlogReader () >>= fun tlogReader ->
  Lwt.return ( tlogWriter, tlogReader )

let teardown (tlogWriter,tlogReader) =
  cleanupFiles () >>= fun() ->
  Lwt.return()

let write_n_set_operations tlogWriter n =
  let rec loop = function
    | i when i=n -> Lwt.return()
    | i ->
	let key = Printf.sprintf "key_%s" (Sn.string_of i) 
	and value = Printf.sprintf "value_%s" (Sn.string_of i) in
	let update = Update.Set(key, value) in
        tlogWriter # log_update i update >>= fun( wrResult ) ->
        loop (Sn.succ i)
  in loop 0L

let test_iterate (tlogWriter, tlogReader) =
  write_n_set_operations tlogWriter 50L >>= fun () ->
  let result = ref [] in 
  let add_to e = 
    let () = result := e :: ! result  in Lwt.return () 
  in
  tlogReader # iterate 27L 30L add_to >>= fun () ->
  let rl = List.length !result in
  Lwt_log.info_f "rl=%i" rl >>= fun () ->
  OUnit.assert_equal rl 4;
  Lwt.return ()


let test_iterate_repetitions (tlogWriter, tlogReader) = 
  let i0 = Sn.start in
  tlogWriter # log_update i0 (Update.Set("k","1")) >>= fun _ ->
  tlogWriter # log_update i0 (Update.Set("k","2")) >>= fun _ ->
  tlogWriter # log_update i0 (Update.Set("k","3")) >>= fun _ ->
  let i1 = Sn.succ i0 in
  tlogWriter # log_update i1 (Update.Set("k","4")) >>= fun _ ->
  tlogWriter # log_update i1 (Update.Set("k","5")) >>= fun _ ->
  tlogWriter # log_update i1 (Update.Set("k","6")) >>= fun _ ->
  let dump e = 
    let i,u = e in
    Lwt_io.printlf "%s:%s" (Sn.string_of i) (Update.string_of u)
  in
  tlogReader # iterate 0L 0L dump >>= fun () ->
  Lwt.return ()



let test_read_next_entry (tlogWriter,tlogReader)  =
  let testI = Sn.start in
  let u = Update.Set("key","value") in
  tlogWriter # log_update testI u >>= fun wrResult  ->
  tlogReader # readNextEntry () >>= fun (i, update ) ->
  begin
    match update with
      | Update.Set("key","value") -> Lwt.return ()
      | _ -> Llio.lwt_failfmt "wrong update %s" (Update.string_of update)
  end >>= fun () ->
  if (i <> testI)
  then Llio.lwt_failfmt "Got wrong value for i : %s (expected %s)" (Sn.string_of i) (Sn.string_of testI)
  else Lwt.return ()


let test_process_last_entries ( tlogWriter, tlogReader ) =
  write_n_set_operations tlogWriter 100L >>= fun () ->
  validateTlogDump 10 "0dfbe8aa4c20b52e1b8bf3cb6cbdf193"

let suite =
  let wrapTest f = lwt_bracket setup f teardown  in
  "tlog_test" >::: [
    "log_delete" >:: wrapTest test_log_delete;
    "log_set" >:: wrapTest test_log_set; 
    "read_next_entry" >:: wrapTest test_read_next_entry; 
    "validate_empty" >:: wrapTest test_validate_empty; 
    "validate_tlog" >:: wrapTest test_validate_tlog;
    "iterate" >:: wrapTest test_iterate;  
    "iterate_repetitions" >:: wrapTest test_iterate_repetitions; 
    "test_process_last_entries" >:: wrapTest test_process_last_entries; 
  ]


