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

let change_to_update =
  let open Update in
  let rec f = function
    | Set (k, v) -> Update.Set (k, v)
    | Delete k -> Update.Delete k
    | Assert (k, vo) -> Update.Assert (k, vo)
    | Assert_exists k -> Update.Assert_exists k
    | TestAndSet (k, vo, v) -> Update.TestAndSet (k, vo, v)
    | Sequence l -> Update.Sequence (List.map f l)
  in
  f

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
      Client.request ic oc Protocol.Confirm (key, value) >>=
      compat

    method aSSert ?(consistency=Consistent) key vo =
      Client.request ic oc Protocol.Assert (consistency, key, vo) >>=
      compat

    method aSSert_exists ?(consistency=Consistent) key =
      Client.request ic oc Protocol.Assert_exists (consistency, key) >>=
      compat

    method delete key =
      Client.request ic oc Protocol.Delete key >>=
      compat

    method delete_prefix prefix =
      Client.request ic oc Protocol.Delete_prefix prefix >>=
      compat

    method range ?(consistency=Consistent) first finc last linc max =
      Client.request ic oc Protocol.Range (consistency, RangeRequest.t ~first ~finc ~last ~linc ~max) >>= fun r ->
      let f a =
        let a' = ReversedArray.array a in
        let rec loop acc = function
          | 0 -> acc
          | n -> begin
              let v = Array.get a' (n - 1) in
              let k = Key.sub v 0 (Key.length v) in
              loop (k :: acc) (n - 1)
          end
        in
        loop [] (Array.length a')
      in
      compat (Result.map f r)

    method range_entries ?(consistency=Consistent) ~first ~finc ~last ~linc ~max =
      Client.request ic oc Protocol.Range_entries (consistency, RangeRequest.t ~first ~finc ~last ~linc ~max) >>= fun r ->
      let f cl =
        List.map (fun (k, v) -> (Key.sub k 0 (Key.length k), v)) (CountedList.list cl)
      in
      compat (Result.map f r)

    method rev_range_entries ?(consistency=Consistent) ~first ~finc ~last ~linc ~max =
      Client.request ic oc Protocol.Rev_range_entries (consistency, RangeRequest.t ~first ~finc ~last ~linc ~max) >>= fun r ->
      let f cl =
        List.map (fun (k, v) -> (Key.sub k 0 (Key.length k), v)) (CountedList.list cl)
      in
      compat (Result.map f r)

    method prefix_keys ?(consistency=Consistent) pref max =
      Client.request ic oc Protocol.Prefix_keys (consistency, pref, max) >>= fun r ->
      let f cl =
        List.map (fun k -> Key.sub k 0 (Key.length k)) (CountedList.list cl)
      in
      compat (Result.map f r)

    method test_and_set key expected wanted =
      Client.request ic oc Protocol.Test_and_set (key, expected, wanted) >>=
      compat

    method replace key wanted =
      Client.request ic oc Protocol.Replace (key, wanted) >>=
      compat

    method user_function name po =
      Client.request ic oc Protocol.User_function (name, po) >>=
      compat

    method multi_get ?(consistency=Consistent) keys =
      let keys' = CountedList.of_list keys in
      Client.request ic oc Protocol.Multi_get (consistency, keys') >>=
      compat

    method multi_get_option ?(consistency=Consistent) keys =
      let keys' = CountedList.of_list keys in
      Client.request ic oc Protocol.Multi_get_option (consistency, keys') >>=
      compat

    method sequence changes =
      let changes' = Update.Update.Sequence (List.map change_to_update changes) in
      Client.request ic oc Protocol.Sequence changes' >>=
      compat

    method synced_sequence changes =
      let changes' = Update.Update.Sequence (List.map change_to_update changes) in
      Client.request ic oc Protocol.Synced_sequence changes' >>=
      compat

    method who_master () =
      Client.request ic oc Protocol.Who_master () >>=
      compat

    method expect_progress_possible () =
      Client.request ic oc Protocol.Expect_progress_possible () >>=
      compat

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
      Client.request ic oc Protocol.Get_key_count () >>=
      compat

    method get_cluster_cfgs () =
      Common.get_nursery_cfg conn

    method version () =
      Client.request ic oc Protocol.Version () >>=
      compat

    method current_state () =
      Client.request ic oc Protocol.Current_state () >>=
      compat
    method nop () =
      Client.request ic oc Protocol.Nop () >>=
      compat
    method get_txid () =
      Client.request ic oc Protocol.Get_txid () >>=
      compat

end: Arakoon_client.client )

let make_remote_client cluster connection =
  Common.prologue cluster connection >>= fun () ->
  let client = new remote_client connection in
  let ac = (client :> Arakoon_client.client) in
  Lwt.return ac
