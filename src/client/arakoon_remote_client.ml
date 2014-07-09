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

open Lwt
open Common
open Statistics
open Arakoon_client

open Arakoon_protocol

let compat =
    let open Arakoon_protocol.Result in
    let open Arakoon_exc in
    let error c s = Lwt.fail (Exception (c, s)) in
    function
      | Ok a -> Lwt.return a
      | No_magic s -> error E_NO_MAGIC s
      | No_hello s -> error E_NO_HELLO s
      | Not_master s -> error E_NOT_MASTER s
      | Result.Not_found s -> error E_NOT_FOUND s
      | Wrong_cluster s -> error E_WRONG_CLUSTER s
      | Assertion_failed s -> error E_ASSERTION_FAILED s
      | Read_only s -> error E_READ_ONLY s
      | Outside_interval s -> error E_OUTSIDE_INTERVAL s
      | Going_down s -> error E_GOING_DOWN s
      | Not_supported s -> error E_NOT_SUPPORTED s
      | No_longer_master s -> error E_NO_LONGER_MASTER s
      | Bad_input s -> error E_BAD_INPUT s
      | Inconsistent_read s -> error E_INCONSISTENT_READ s
      | Userfunction_failure s -> error E_USERFUNCTION_FAILURE s
      | Max_connections s -> error E_MAX_CONNECTIONS s
      | Unknown_failure s -> error E_UNKNOWN_FAILURE s

class remote_client ((ic,oc) as conn) =

  (object
    method exists ?(consistency=Consistent) key =
      Client.request ic oc Protocol.Exists (consistency, key) >>=
      compat

    method get ?(consistency=Consistent) key =
      Client.request ic oc Protocol.Get (consistency, key) >>=
      compat

    method set key value =
      Client.request ic oc Protocol.Set (key, value) >>=
      compat

    method confirm key value =
      request  oc (fun buf -> confirm_to buf key value) >>= fun () ->
      response ic nothing

    method aSSert ?(consistency=Consistent) key vo =
      Client.request ic oc Protocol.Assert (consistency, key, vo) >>=
      compat

    method aSSert_exists ?(consistency=Consistent) key =
      Client.request ic oc Protocol.Assert_exists (consistency, key) >>=
      compat

    method delete key =
      Client.request ic oc Protocol.Delete key >>=
      compat

    method delete_prefix prefix = Common.delete_prefix conn prefix

    method range ?(consistency=Consistent) first finc last linc max =
      request oc (fun buf -> range_to buf ~consistency first finc last linc max)
      >>= fun () ->
      response ic Llio.input_string_list

    method range_entries ?(consistency=Consistent) ~first ~finc ~last ~linc ~max =
      request oc (fun buf -> range_entries_to buf ~consistency first finc last linc max)
      >>= fun () ->
      response ic Llio.input_kv_list

    method rev_range_entries ?(consistency=Consistent) ~first ~finc ~last ~linc ~max =
      request oc (fun buf -> rev_range_entries_to buf ~consistency first finc last linc max)
      >>= fun () ->
      response ic Llio.input_kv_list

    method prefix_keys ?(consistency=Consistent) pref max =
      request  oc (fun buf -> prefix_keys_to buf ~consistency pref max) >>= fun () ->
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
      request  oc (fun buf -> multiget_to buf ~consistency keys) >>= fun () ->
      response ic
        (fun ic -> Llio.input_string_list ic >>= fun x ->
          Lwt.return (List.rev x))

    method multi_get_option ?(consistency=Consistent) keys =
      request oc (fun buf -> multiget_option_to buf ~consistency keys) >>= fun () ->
      response ic (Llio.input_list Llio.input_string_option)

    method sequence changes = Common.sequence conn changes

    method synced_sequence changes = Common.synced_sequence conn changes

    method who_master () =
      Client.request ic oc Protocol.Who_master () >>=
      compat

    method expect_progress_possible () =
      request  oc (fun buf -> expect_progress_possible_to buf) >>= fun () ->
      response ic Llio.input_bool

    method statistics () =
      request oc (fun buf -> command_to buf STATISTICS) >>= fun () ->
      response ic
        (fun ic -> Llio.input_string ic >>= fun ss ->
          let s = Statistics.from_buffer (Llio.make_buffer ss 0) in
          Lwt.return s
        )

    method ping client_id cluster_id =
      Client.request ic oc Protocol.Ping (client_id, cluster_id) >>=
      compat

    method get_key_count () =
      request  oc (fun buf -> get_key_count_to buf ) >>= fun () ->
      response ic Llio.input_int64

    method get_cluster_cfgs () =
      Common.get_nursery_cfg conn

    method version () =
      Common.version conn

    method current_state () = Common.current_state conn
    method nop () =
      Client.request ic oc Protocol.Nop () >>=
      compat
    method get_txid () = Common.get_txid conn

end: Arakoon_client.client )

let make_remote_client cluster connection =
  Common.prologue cluster connection >>= fun () ->
  let client = new remote_client connection in
  let ac = (client :> Arakoon_client.client) in
  Lwt.return ac
