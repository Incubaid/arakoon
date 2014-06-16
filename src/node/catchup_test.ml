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
open Lwt
open Extra
open Update

let section = Logger.Section.main

module S = (val (Store.make_store_module (module Batched_store.Local_store)))

let _dir_name = "/tmp/catchup_test"
let _tlf_dir = "/tmp/catchup_test/tlf"
  
let _fill tlog_coll n = 
  let sync = false in
  let rec _loop i = 
    if i = n
    then Lwt.return ()
    else
      begin
	    let k = Printf.sprintf "key%i" i
	    and v = Printf.sprintf "value%i" i in
	    let u = Update.Set (k,v) in
        let value = Value.create_client_value [u] sync in
	    tlog_coll # log_value (Sn.of_int i) value >>= fun () ->
	    _loop (i+1)
      end
  in
  _loop 0

let _fill2 tlog_coll n = 
  let sync = false in
  let rec _loop i =
    if i = n then Lwt.return ()
    else
      begin 
	    let k = Printf.sprintf "_2_key%i" i
	    and v = Printf.sprintf "_2_value%i" i 
	    and k2 = Printf.sprintf "key%i" i 
	    and v2 = Printf.sprintf "value%i" i 
	    in
	    let u = Update.Set(k,v) in
	    let u2 = Update.Set(k2,v2) in
        let value = Value.create_client_value [u] sync in
        let value2 = Value.create_client_value [u2] sync in
        let sni = Sn.of_int i in
	    tlog_coll # log_value  sni value  >>= fun () ->
	    tlog_coll # log_value  sni value2 >>= fun () ->
	    _loop (i+1)
      end
  in
  _loop 0

let _fill3 tlog_coll n =
  let sync = false in
  let rec _loop i =
    if i = n then Lwt.return ()
    else
      begin
        let k = Printf.sprintf "_3a_key%i" i
        and v = Printf.sprintf "_3a_value%i" i
        and k2 = Printf.sprintf "key%i" i
        and v2 = Printf.sprintf "value%i" i
        and k3 = Printf.sprintf "_3b_key%i" i
        and v3 = Printf.sprintf "_3b_value%i" i
        in
        let u = Update.Set(k,v) in
        let u2 = Update.Set(k2,v2) in
        let u3 = Update.Sequence [Update.Set(k3,v3); Update.Assert_exists("nonExistingKey")] in
        let value = Value.create_client_value [u; u3] sync in
        let value2 = Value.create_client_value [u2; u3] sync in
        let sni = Sn.of_int i in
        tlog_coll # log_value sni value  >>= fun () ->
        tlog_coll # log_value sni value2 >>= fun () ->
        _loop (i+1)
      end
  in
  _loop 0

let ignore_ex f = 
  Lwt.catch 
    f
    (fun exn -> Logger.warning_ ~exn "ignoring")

let setup () = 
  Logger.info_ "Catchup_test.setup" >>= fun () ->
  ignore_ex (fun () -> File_system.rmdir _tlf_dir) >>= fun () ->
  ignore_ex (fun () -> File_system.rmdir _dir_name) >>= fun () ->
  ignore_ex (fun () -> File_system.mkdir  _dir_name 0o750 ) >>= fun () ->
  ignore_ex (fun () -> File_system.mkdir  _tlf_dir 0o750 )

    
let test_common () =
  Logger.info_ "test_common" >>= fun () ->
  Tlc2.make_tlc2 _dir_name _tlf_dir _tlf_dir true false "node_name" >>= fun tlog_coll ->
  _fill tlog_coll 1000 >>= fun () ->
  let me = "" in
  let db_name = _dir_name ^ "/my_store1.db" in
  S.make_store db_name >>= fun store ->
  Catchup.catchup_store me ((module S),store,tlog_coll) 500L >>= fun() ->
  Logger.info_ "TODO: validate store after this" >>= fun ()->
  S.close store >>= fun () ->
  tlog_coll # close ()


let teardown () = 
  Logger.info_ "Catchup_test.teardown" >>= fun () ->
  let clean_dir dir =
    Lwt.catch
      (fun () ->
        File_system.lwt_directory_list dir >>= fun entries ->
        Lwt_list.iter_s (fun i ->
	      let fn = dir ^ "/" ^ i in
          ignore_ex (fun () -> Lwt_unix.unlink fn)) entries
      )
      (fun exn -> Logger.debug_ ~exn "ignoring" ) in
  clean_dir _tlf_dir >>= fun () ->
  clean_dir _dir_name >>= fun () ->
  Logger.debug_ "end of teardown"

let _tic (type s) filler_function n name verify_store =
  Tlogcommon.tlogEntriesPerFile := 101;
  Tlc2.make_tlc2 _dir_name _tlf_dir _tlf_dir true false "node_name" >>= fun tlog_coll ->
  filler_function tlog_coll n >>= fun () ->
  let db_name = _dir_name ^ "/" ^ name ^ ".db" in
  S.make_store db_name >>= fun store ->
  let me = "??me??" in
  Catchup.verify_n_catchup_store me ((module S), store, tlog_coll)
  >>= fun () ->
  let new_i = S.get_succ_store_i store in
  verify_store store new_i >>= fun () ->
  Logger.info_f_ "new_i=%s" (Sn.string_of new_i) >>= fun () ->
  tlog_coll # close () >>= fun () -> 
  S.close store



let test_interrupted_catchup () =
  Logger.info_ "test_interrupted_catchup" >>= fun () ->
  _tic _fill 1000 "tic" (fun store new_i -> Lwt.return ())


let test_with_doubles () =
  Logger.info_ "test_with_doubles" >>= fun () ->
  _tic _fill2 1000 "twd" (fun store new_i -> Lwt.return ())


let test_batched_with_failures () =
  Logger.info_ "test_batched_with_failures" >>= fun () ->
  _tic _fill3 3000 "tbwf"
    (fun store new_i ->
      let assert_not_exists k = S.exists store k >>= fun exists -> if exists then failwith "found key that is not supposed to be in the store!" else Lwt.return () in
      let assert_exists k = S.exists store k >>= fun exists -> if not exists then failwith "could not find required key in the store!" else Lwt.return () in
      assert_exists "key2" >>= fun () ->
      assert_exists "key2590" >>= fun () ->
      assert_not_exists "_3a_key2" >>= fun () ->
      assert_not_exists "_3b_key2" >>= fun () ->
      assert_not_exists "_3a_key200" >>= fun () ->
      assert_not_exists "_3b_key200" >>= fun () ->
      assert_not_exists "_3a_key2530" >>= fun () ->
      assert_not_exists "_3b_key2530")

let test_large_tlog_catchup () =
  _tic _fill 100_000 "tcs"
    (fun store new_i -> Lwt.return ())

let suite =
  let w f = lwt_bracket setup f teardown in
  "catchup" >:::[
    "common" >:: w test_common;
    "with_doubles" >:: w test_with_doubles;
    "interrupted_catchup" >:: w test_interrupted_catchup;
    "batched_with_failures" >:: w test_batched_with_failures;
    "large_tlog_catchup" >:: w test_large_tlog_catchup;
  ]

