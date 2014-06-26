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

(** Wrappers for {! Ssl } which track the kind of contexts in the type system *)

(** Kind-annotated wrapper for {! Ssl.t } *)
type 'a t

(** Create a {i client } context

    This wraps {! Ssl.create_context } passing {! Ssl.Client_context }. *)
val create_client_context : Ssl.protocol -> [ `Client ] t

(** Create a {i server } context

    This wraps {! Ssl.create_context } passing {! Ssl.Server_context }. *)
val create_server_context : Ssl.protocol -> [ `Server ] t

(** Create a {i both } context

    This wraps {! Ssl.create_context } passing {! Ssl.Both_context }. *)
val create_both_context : Ssl.protocol -> [ `Client | `Server ] t


(** Wrapper for {! Ssl.embed_socket } *)
val embed_socket : Unix.file_descr -> 'a t -> Ssl.socket


(** Wrapper for {! Ssl.use_certificate } *)
val use_certificate : 'a t -> string -> string -> unit

(** Wrapper for {! Ssl.set_verify } *)
val set_verify : 'a t -> Ssl.verify_mode list -> Ssl.verify_callback option -> unit

(** Wrapper for {! Ssl.set_client_CA_list_from_file } *)
val set_client_CA_list_from_file : [> `Server ] t -> string -> unit

(** Wrapper for {! Ssl.load_verify_locations } *)
val load_verify_locations : 'a t -> string -> string -> unit

(** Wrapper for {! Ssl.set_cipher_list } *)
val set_cipher_list : 'a t -> string -> unit


(** Bindings to {! Lwt_ssl } functions *)
module Lwt : sig
  (** Wrapper for {! Lwt_ssl.ssl_connect }

      @raise Failure {! Lwt_ssl.ssl_connect } returned a plain socket *)
  val ssl_connect : Lwt_unix.file_descr -> [> `Client ] t -> (Ssl.socket * Lwt_ssl.socket) Lwt.t

  (** Wrapper for {! Lwt_ssl.ssl_accept }

      @raise Failure {! Lwt_ssl.ssl_accept } returned a plain socket *)
  val ssl_accept : Lwt_unix.file_descr -> [> `Server ] t -> (Ssl.socket * Lwt_ssl.socket) Lwt.t
end
