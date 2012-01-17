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
open Common
open Lwt_log
open Update
open Statistics




class remote_client ((ic,oc) as conn) =

object(self: #Arakoon_client.client)

  method exists ?(allow_dirty=false) key =
    request  oc (fun buf -> exists_to ~allow_dirty buf key) >>= fun () ->
    response ic Llio.input_bool

  method get ?(allow_dirty=false) key = Common.get conn ~allow_dirty key

  method set key value = Common.set conn key value

  method confirm key value =
    request  oc (fun buf -> confirm_to buf key value) >>= fun () ->
    response ic nothing

  method aSSert ?(allow_dirty=false) key vo =
    request oc (fun buf -> assert_to ~allow_dirty buf key vo) >>= fun () ->
    response ic nothing

  method delete key =
    request  oc (fun buf -> delete_to buf key) >>= fun () ->
    response ic nothing

  method range ?(allow_dirty=false) first finc last linc max =
    request oc (fun buf -> range_to buf ~allow_dirty first finc last linc max)
    >>= fun () ->
    response ic Llio.input_string_list

  method range_entries ?(allow_dirty=false) ~first ~finc ~last ~linc ~max =
    request oc (fun buf -> range_entries_to buf ~allow_dirty first finc last linc max)
    >>= fun () ->
    response ic Llio.input_kv_list

  method rev_range_entries ?(allow_dirty=false) ~first ~finc ~last ~linc ~max =
    request oc (fun buf -> rev_range_entries_to buf ~allow_dirty first finc last linc max)
    >>= fun () ->
    response ic Llio.input_kv_list

  method prefix_keys ?(allow_dirty=false) pref max =
    request  oc (fun buf -> prefix_keys_to buf ~allow_dirty pref max) >>= fun () ->
    response ic Llio.input_string_list

  method test_and_set key expected wanted =
    request  oc (fun buf -> test_and_set_to buf key expected wanted) >>= fun () ->
    response ic Llio.input_string_option

  method user_function name po =
    request  oc (fun buf -> user_function_to buf name po) >>= fun () ->
    response ic Llio.input_string_option

  method multi_get ?(allow_dirty=false) keys =
    request  oc (fun buf -> multiget_to buf ~allow_dirty keys) >>= fun () ->
    response ic Llio.input_string_list

  method sequence changes = Common.sequence conn changes


  method who_master () = Common.who_master conn

  method expect_progress_possible () =
    request  oc (fun buf -> expect_progress_possible_to buf) >>= fun () ->
    response ic Llio.input_bool

  method statistics () =
    request oc (fun buf -> command_to buf STATISTICS) >>= fun () ->
    response ic
      (fun ic -> Llio.input_string ic >>= fun ss ->
	let s,_  = Statistics.from_buffer ss 0 in
	Lwt.return s
      )

  method ping client_id cluster_id =
    request  oc (fun buf -> ping_to buf client_id cluster_id) >>= fun () ->
    response ic Llio.input_string

  method get_key_count () =
    request  oc (fun buf -> get_key_count_to buf ) >>= fun () ->
    response ic Llio.input_int64

  method get_cluster_cfgs () =
    Common.get_nursery_cfg (ic,oc)
end

let make_remote_client cluster connection =
  Common.prologue cluster connection >>= fun () ->
  let client = new remote_client connection in
  let ac = (client :> Arakoon_client.client) in
  Lwt.return ac
