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


class remote_client (ic,oc) =
  let request f =
    let buf = Buffer.create 32 in
    let () = f buf in
      Lwt_io.write oc (Buffer.contents buf) >>= fun () ->
      Lwt_io.flush oc
  in
object(self: #Arakoon_client.client)

  method exists ?(allow_dirty=false) key =
    request (fun buf -> exists_to ~allow_dirty buf key) >>= fun () ->
    response ic Llio.input_bool

  method get ?(allow_dirty=false) key =
    request (fun buf -> get_to ~allow_dirty buf key) >>= fun () ->
    response ic Llio.input_string

  method set key value =
    request (fun buf -> set_to buf key value) >>= fun () ->
    response ic nothing

  method confirm key value = 
    request (fun buf -> confirm_to buf key value) >>= fun () ->
    response ic nothing

  method aSSert ?(allow_dirty=false) key vo = 
    request (fun buf -> assert_to ~allow_dirty buf key vo) >>= fun () ->
    response ic nothing

  method delete key =
    request (fun buf -> delete_to buf key) >>= fun () ->
    response ic nothing

  method range ?(allow_dirty=false) first finc last linc max =
    request (fun buf -> range_to buf ~allow_dirty first finc last linc max) >>= fun () ->
    response ic key_list

  method range_entries ?(allow_dirty=false) ~first ~finc ~last ~linc ~max =
    request (fun buf -> range_entries_to buf ~allow_dirty first finc last linc max)
    >>= fun () ->
    response ic kv_list

  method prefix_keys ?(allow_dirty=false) pref max =
    request (fun buf -> prefix_keys_to buf ~allow_dirty pref max) >>= fun () ->
    response ic key_list

  method test_and_set key expected wanted =
    request (fun buf -> test_and_set_to buf key expected wanted) >>= fun () ->
    response ic Llio.input_string_option

  method user_function name po = 
    request (fun buf -> user_function_to buf name po) >>= fun () ->
    response ic Llio.input_string_option

  method multi_get ?(allow_dirty=false) keys = 
    request (fun buf -> multiget_to buf ~allow_dirty keys) >>= fun () ->
    response ic value_list

  method sequence changes = 
    let outgoing buf = 
      command_to buf SEQUENCE;
      let update_buf = Buffer.create (32 * List.length changes) in
      let rec c2u = function
	| Arakoon_client.Set (k,v) -> Update.Set(k,v)
	| Arakoon_client.Delete k -> Update.Delete k
	| Arakoon_client.TestAndSet (k,vo,v) -> Update.TestAndSet (k,vo,v)
	| Arakoon_client.Sequence cs -> Update.Sequence (List.map c2u cs)
	| Arakoon_client.Assert(k,vo) -> Update.Assert(k,vo)
      in
      let updates = List.map c2u changes in
      let seq = Update.Sequence updates in
      let () = Update.to_buffer update_buf seq in
      let () = Llio.string_to buf (Buffer.contents update_buf)
      in () 
    in
    request (fun buf -> outgoing buf) >>= fun () ->
    response ic nothing

  method who_master () =
    request (fun buf -> who_master_to buf) >>= fun () ->
    response ic Llio.input_string_option

  method expect_progress_possible () = 
    request (fun buf -> expect_progress_possible_to buf) >>= fun () ->
    response ic Llio.input_bool

  method statistics () = 
    request (fun buf -> command_to buf STATISTICS) >>= fun () ->
    response ic 
      (fun ic -> Llio.input_string ic >>= fun ss ->
	let s,_  = Statistics.from_buffer ss 0 in
	Lwt.return s
      )

  method ping client_id cluster_id =
    request (fun buf -> ping_to buf client_id cluster_id) >>= fun () ->
    response ic Llio.input_string

  method get_key_count () =
    request (fun buf -> get_key_count_to buf ) >>= fun () ->
    response ic Llio.input_int64
end

let prologue cluster connection =
  let (_,oc) = connection in 
  Llio.output_int32  oc _MAGIC >>= fun () ->
  Llio.output_int    oc _VERSION >>= fun () ->
  Llio.output_string oc cluster 

let make_remote_client cluster connection = 
  prologue cluster connection >>= fun () ->
  let client = new remote_client connection in
  let ac = (client :> Arakoon_client.client) in
  Lwt.return ac
