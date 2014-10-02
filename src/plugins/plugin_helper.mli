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


val serialize_string  : Buffer.t -> string -> unit
val serialize_hashtbl : Buffer.t -> (Buffer.t -> 'a -> 'b -> unit) ->
                        ('a, 'b) Hashtbl.t -> unit

val serialize_string_list: Buffer.t -> string list -> unit

type input
val make_input : string -> int -> input

val deserialize_string : input -> string
val deserialize_hashtbl: input -> (input -> 'a * 'b) -> ('a, 'b) Hashtbl.t
val deserialize_string_list: input -> string list

val debug_f: ('a, unit, string, unit) format4 -> 'a
