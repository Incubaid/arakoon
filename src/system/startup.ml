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

(*
let test_t make_config name =
  let cfgs, forced_master, quorum_function, 
    lease_expiry, 
    use_compression = 
    make_config () 
  in
  let make_store = Mem_store.make_mem_store in
  let make_tlog_coll = Mem_tlogcollection.make_mem_tlog_collection in
  let get_cfgs () = cfgs in
  _main_2 make_store make_tlog_coll get_cfgs
    forced_master quorum_function name
    false lease_expiry
*)

let post_failure () = 
  let cfgs = [] in
  let lease_expiration = 10 in
  let use_compression  = true in
  let forced_master    = Some "master" in
  let quorum_function  n = (n/2) +1 in
  let make_store = Mem_store.make_mem_store in
  let make_tlog_coll = Mem_tlogcollection.make_mem_tlog_collection in
  let get_cfgs () = cfgs in
  OUnit.assert_equal "ok" "nope";
  Node_main._main_2 
    make_store 
    make_tlog_coll 
    get_cfgs
    forced_master
    quorum_function 
    "master"
    use_compression
    lease_expiration 
  >>= fun () ->
  Lwt.return ()
    




let setup () = Lwt.return ()
let teardown () = Lwt.return ()

let w f = Extra.lwt_bracket setup f teardown 

let suite = "startup" >:::[
  "post_failure" >:: (fun () -> OUnit.todo "implement & enable");
]
