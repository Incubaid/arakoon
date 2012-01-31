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

let _dir_name = "/tmp/catchup_test"
  
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
	tlog_coll # log_update (Sn.of_int i) u ~sync >>= fun _ ->
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
	tlog_coll # log_update (Sn.of_int i) u ~sync  >>= fun _ ->
	tlog_coll # log_update (Sn.of_int i) u2 ~sync >>= fun _ ->
	_loop (i+1)
      end
  in
  _loop 0

let setup () = 
  Lwt_log.info "Catchup_test.setup" >>= fun () ->
  let ignore_ex f = 
    Lwt.catch 
      f
      (fun exn -> Lwt_log.warning ~exn "ignoring")
  in
  ignore_ex (fun () -> File_system.rmdir _dir_name) >>= fun () ->
  ignore_ex (fun () -> File_system.mkdir  _dir_name 0o750 )

    
let test_common () =
  Lwt_log.info "test_common" >>= fun () ->
  Tlc2.make_tlc2 _dir_name true>>= fun tlog_coll ->
  _fill tlog_coll 1000 >>= fun () ->
  let me = "" in
  let db_name = _dir_name ^ "/my_store1.db" in
  Local_store.make_local_store db_name >>= fun store ->
  Catchup.catchup_store me (store,tlog_coll) 500L >>= fun(end_i,vo) ->
  Lwt_log.info "TODO: validate store after this" >>= fun ()->
  tlog_coll # close () >>= fun () ->
  store # close () 


let teardown () = 
  Lwt_log.info "Catchup_test.teardown" >>= fun () ->
  Lwt.catch
    (fun () ->
      File_system.lwt_directory_list _dir_name >>= fun entries ->
      Lwt_list.iter_s (fun i -> 
	let fn = _dir_name ^ "/" ^ i in
        Lwt_unix.unlink fn) entries 
    )
    (fun exn -> Lwt_log.debug ~exn "ignoring" )
    >>= fun () ->
  Lwt_log.debug "end of teardown"

let _tic filler_function name =
  Tlogcommon.tlogEntriesPerFile := 101; 
  Tlc2.make_tlc2 _dir_name true>>= fun tlog_coll ->
  filler_function tlog_coll 1000 >>= fun () ->
  let tlog_i = Sn.of_int 1000 in 
  let db_name = _dir_name ^ "/" ^ name ^ ".db" in
  Local_store.make_local_store db_name >>= fun store ->
  let me = "??me??" in
  Catchup.verify_n_catchup_store me (store, tlog_coll, Some tlog_i) tlog_i None
  >>= fun (new_i,vo) ->
  Lwt_log.info_f "new_i=%s" (Sn.string_of new_i) >>= fun () ->
  tlog_coll # close () >>= fun () -> 
  store # close () 



let test_interrupted_catchup () =
  Lwt_log.info "test_interrupted_catchup" >>= fun () ->
  _tic _fill "tic"


let test_with_doubles () =
  Lwt_log.info "test_with_doubles" >>= fun () ->
  _tic _fill2 "twd"

let suite = 
  let w f = lwt_bracket setup f teardown in
  "catchup" >:::[
    "common" >:: w test_common;
    "with_doubles" >:: w test_with_doubles; 
    "interrupted_catchup" >:: w test_interrupted_catchup;

  ]
