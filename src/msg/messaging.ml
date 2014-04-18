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
open Log_extra
open Message
open Lwt_buffer

type id = string
type address = (string * int)

class type messaging = object
  method register_receivers: (id * address) list -> unit
  method send_message: Message.t -> source:id -> ?sub_target:string -> target:id -> unit Lwt.t
  method recv_message: ?sub_target:string -> target:id -> (Message.t * id) Lwt.t
  method expect_reachable: target: id -> bool
  method run :
    ?setup_callback:(unit -> unit Lwt.t) ->
    ?teardown_callback:(unit -> unit Lwt.t) ->
    ?ssl_context:([> `Server ] Typed_ssl.t) ->
    unit -> unit Lwt.t
  method get_buffer: ?sub_target:string -> id -> (Message.t * id) Lwt_buffer.t
end
