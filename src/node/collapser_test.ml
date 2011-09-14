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
open OUnit
open Update

let _should_fail x error_msg success_msg =
  Lwt.catch 
    (fun ()  -> 
      x () >>= fun () -> 
      Lwt_log.debug "should fail...doesn't" >>= fun () ->
      Lwt.return true) 
    (fun exn -> Lwt_log.debug ~exn success_msg >>= fun () -> Lwt.return false)
  >>= fun bad -> 
  if bad then Lwt.fail (Failure error_msg)
  else Lwt.return ()

let _make_updates tlc n =
  let rec loop i =
    if i = n
    then Lwt.return ()
    else 
      let a = i mod 10000 in
      let key = Printf.sprintf "sqrt(%i)" a in
      let value = Printf.sprintf "%f" (sqrt (float a)) in
      let update = Update.Set(key, value) 
      in
      let sni = Sn.of_int i in
      tlc # log_update sni update >>= fun wr_result ->
      loop (i+1)
  in
  loop 0

let test_collapse_until dn = 
  let () = Tlogcommon.tlogEntriesPerFile := 1000 in
  Lwt_log.debug_f "dn=%s" dn >>= fun () ->
  Tlc2.make_tlc2 dn true >>= fun tlc ->
  _make_updates tlc 1111 >>= fun () ->
  tlc # close () >>= fun () ->
  Lwt_unix.sleep 5.0 >>= fun () -> (* give it time to generate the .tlc *)
  (* now collapse first file into a tc *)
  let storename = "head.db" in
  begin
    File_system.exists storename >>= fun head_exists ->
      if head_exists
      then
        File_system.unlink storename
      else
        Lwt.return ()
  end
  >>= fun () ->
  let store_methods = (Local_store.make_local_store, Local_store.copy_store, storename) in
  let future_i = Sn.of_int 1001 in
  let cb = fun s -> Lwt.return () in
  Collapser.collapse_until tlc store_methods future_i cb >>= fun () ->
  (* some verification ? *)
  
  (* try to do it a second time, it should *)
  let future_i2 = Sn.of_int 1000 in
    _should_fail 
    (fun () ->
      Collapser.collapse_until tlc store_methods future_i2 cb) 
    "this should fail" 
    "great, it indeed refuses to do this" 
  >>= fun ()->
  Lwt.return ()


let test_dn = "/tmp/collapser" 

let test_collapse_many dn =
  let () = Tlogcommon.tlogEntriesPerFile := 100 in
  Lwt_log.debug_f "test_collapse_many_regime dn=%s" dn >>= fun () ->
  Tlc2.make_tlc2 dn true >>= fun tlc ->
  _make_updates tlc 632 >>= fun () ->
  tlc # close () >>= fun () ->
  Lwt_unix.sleep 5.0 >>= fun () -> (* compression finished ? *) 
  let storename = Filename.concat test_dn "head.db" in
  let cb fn = Lwt_log.debug_f "collapsed %s" (Sn.string_of fn) in
  let cb' = fun n -> Lwt.return () in
  begin
    File_system.exists storename >>= fun head_exists ->
	  if head_exists
	  then
	    File_system.unlink storename
	  else
	    Lwt.return ()
  end
  >>= fun () ->
  let store_methods = (Local_store.make_local_store, Local_store.copy_store, storename) in
  Collapser.collapse_many tlc store_methods 5 cb' cb >>= fun () ->
  Lwt_log.debug "collapsed 000" >>= fun () ->
  Collapser.collapse_many tlc store_methods 3 cb' cb >>= fun () ->
  Lwt_log.debug "collapsed 001 & 002" >>= fun () ->
  Collapser.collapse_many tlc store_methods 1 cb' cb >>= fun () ->
  Lwt_log.debug "collapsed 003 & 004" >>= fun () -> (* ends @ 510 *)
  Lwt.return ()


let setup () = 
  Lwt_log.info "Collapser_test.setup" >>= fun () ->
  
  let _ = Sys.command (Printf.sprintf "rm -rf '%s'" test_dn) in
  File_system.mkdir test_dn 0o755 >>= fun () -> 
  Lwt.return test_dn


let teardown dn =
  Lwt_log.debug_f "teardown %s" dn

  
let suite =
  let wrapTest f = Extra.lwt_bracket setup f teardown 
  in
  "collapser_test" >:::[
    "collapse_until" >:: wrapTest test_collapse_until;
    "collapse_many" >:: wrapTest test_collapse_many;
  ]
