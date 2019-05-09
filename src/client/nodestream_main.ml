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
open Remote_nodestream
open Lwt
open Network

let optimize_db ~tls ~tcp_keepalive ip port cluster_id =
  let do_it connection =
    make_remote_nodestream cluster_id connection >>= fun client ->
    client # optimize_db ()
  in
  let address = make_address ip port in
  let t () =
    Client_main.with_connection
      ~tls
      ~tcp_keepalive
      address do_it  >>= fun () ->
    Lwt.return 0
  in
  Lwt_main.run( t())


let defrag_db ~tls ~tcp_keepalive ip port cluster_id =
  let do_it connection =
    make_remote_nodestream cluster_id connection >>= fun client ->
    client # defrag_db ()
  in
  let address = make_address ip port in
  let t () =
    Client_main.with_connection ~tls ~tcp_keepalive address do_it >>= fun () ->
    Lwt.return 0
  in
  Lwt_main.run (t ())


let copy_db_to_head ~tls ~tcp_keepalive ip port cluster_id ~tlogs_to_keep =
  let do_it connection =
    make_remote_nodestream cluster_id connection >>= fun client ->
    client # copy_db_to_head tlogs_to_keep
  in
  let address = make_address ip port in
  let t () =
    Client_main.with_connection ~tls ~tcp_keepalive address do_it >>= fun () ->
    Lwt.return 0
  in
  Lwt_main.run (t ())

let get_db ~tls ~tcp_keepalive ip port cluster_id location =
  let do_it connection =
    make_remote_nodestream cluster_id connection >>= fun client ->
    client # get_db location
  in
  let address = make_address ip port in
  let t () =
    Client_main.with_connection ~tls ~tcp_keepalive address do_it  >>= fun () ->
    Lwt.return 0
  in
  Lwt_main.run( t())

let drop_master ~tls ~tcp_keepalive ip port cluster_id =
  let do_it connection =
    make_remote_nodestream cluster_id connection >>= fun client ->
    client # drop_master ()
  in
  let address = make_address ip port in
  let t () =
    Client_main.with_connection ~tls ~tcp_keepalive address do_it >>= fun () ->
    Lwt.return 0
  in
  Lwt_main.run(t ())
