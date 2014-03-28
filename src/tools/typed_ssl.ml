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

type 'a t = Ssl.context

let create_client_context proto = Ssl.create_context proto Ssl.Client_context
let create_server_context proto = Ssl.create_context proto Ssl.Server_context
let create_both_context proto = Ssl.create_context proto Ssl.Both_context

let embed_socket = Ssl.embed_socket

let use_certificate = Ssl.use_certificate
let set_verify = Ssl.set_verify
let set_client_CA_list_from_file = Ssl.set_client_CA_list_from_file
let load_verify_locations = Ssl.load_verify_locations

open Lwt

module Lwt = struct
  let ssl_connect fd ctx =
    Lwt_ssl.ssl_connect fd ctx >>= fun lwt_s ->
    match Lwt_ssl.ssl_socket lwt_s with
      | None -> Lwt.fail (Failure "Typed_ssl.Lwt.ssl_connect: unexpected plain socket")
      | Some s -> Lwt.return (s, lwt_s)

  let ssl_accept fd ctx =
    Lwt_ssl.ssl_accept fd ctx >>= fun lwt_s ->
    match Lwt_ssl.ssl_socket lwt_s with
      | None -> Lwt.fail (Failure "Typed_ssl.Lwt.ssl_accept: unexpected plain socket")
      | Some s -> Lwt.return (s, lwt_s)
end
