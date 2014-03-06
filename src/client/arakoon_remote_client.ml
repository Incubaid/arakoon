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
open Update
open Statistics
open Arakoon_client

class remote_client ((ic,oc) as conn) =

  object(self: #Arakoon_client.client)

    method exists ?(consistency=Consistent) key =
      request  oc (fun buf -> exists_to ~consistency buf key) >>= fun () ->
      response ic Llio.input_bool

    method get ?(consistency=Consistent) key = Common.get conn ~consistency key

    method set key value = Common.set conn key value

    method confirm key value =
      request  oc (fun buf -> confirm_to buf key value) >>= fun () ->
      response ic nothing

    method aSSert ?(consistency=Consistent) key vo =
      request oc (fun buf -> assert_to ~consistency buf key vo) >>= fun () ->
      response ic nothing

    method aSSert_exists ?(consistency=Consistent) key =
      request oc (fun buf -> assert_exists_to ~consistency buf key) >>= fun () ->
      response ic nothing

    method delete key =
      request  oc (fun buf -> delete_to buf key) >>= fun () ->
      response ic nothing

    method delete_prefix prefix = Common.delete_prefix conn prefix

    method range ?(consistency=Consistent) first finc last linc max =
      request oc (fun buf -> range_to buf ~consistency first finc last linc max)
      >>= fun () ->
      response ic Llio.input_string_list

    method range_entries ?(consistency=Consistent) ~first ~finc ~last ~linc ~max =
      request oc (fun buf -> range_entries_to buf consistency first finc last linc max)
      >>= fun () ->
      response ic Llio.input_kv_list

    method rev_range_entries ?(consistency=Consistent) ~first ~finc ~last ~linc ~max =
      request oc (fun buf -> rev_range_entries_to buf consistency first finc last linc max)
      >>= fun () ->
      response ic Llio.input_kv_list

    method prefix_keys ?(consistency=Consistent) pref max =
      request  oc (fun buf -> prefix_keys_to buf consistency pref max) >>= fun () ->
      response ic Llio.input_string_list

    method test_and_set key expected wanted =
      request  oc (fun buf -> test_and_set_to buf key expected wanted) >>= fun () ->
      response ic Llio.input_string_option

    method replace key wanted =
      request oc (fun buf -> replace_to buf key wanted) >>= fun () ->
      response ic Llio.input_string_option

    method user_function name po =
      request  oc (fun buf -> user_function_to buf name po) >>= fun () ->
      response ic Llio.input_string_option

    method multi_get ?(consistency=Consistent) keys =
      request  oc (fun buf -> multiget_to buf consistency keys) >>= fun () ->
      response ic
        (fun ic -> Llio.input_string_list ic >>= fun x ->
          Lwt.return (List.rev x))

    method multi_get_option ?(consistency=Consistent) keys =
      request oc (fun buf -> multiget_option_to buf consistency keys) >>= fun () ->
      response ic (Llio.input_list Llio.input_string_option)

    method sequence changes = Common.sequence conn changes

    method synced_sequence changes = Common.synced_sequence conn changes

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
      Common.get_nursery_cfg conn

    method version () =
      Common.version conn

    method current_state () = Common.current_state conn
    method nop () = Common.nop conn
    method mark () = Common.mark conn
  end

let make_remote_client cluster connection =
  Common.prologue cluster connection >>= fun () ->
  let client = new remote_client connection in
  let ac = (client :> Arakoon_client.client) in
  Lwt.return ac
