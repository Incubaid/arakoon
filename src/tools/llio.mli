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



open Std

type lwtoc = Lwt_io.output_channel
type lwtic = Lwt_io.input_channel


type namedValue =
  | NAMED_INT of string * int
  | NAMED_INT64 of string * int64
  | NAMED_FLOAT of string * float
  | NAMED_STRING of string * string
  | NAMED_VALUELIST of string * (namedValue list)

val lwt_failfmt :  ('a, unit, string, 'b Lwt.t) format4 -> 'a

type buffer
val make_buffer : string -> int -> buffer
val buffer_pos : buffer -> int
val buffer_done : buffer -> bool
val buffer_set_pos: buffer -> int -> unit

type 'a serializer = Buffer.t -> 'a -> unit
type 'a deserializer = buffer -> 'a

type 'a lwt_serializer = lwtoc -> 'a -> unit Lwt.t
type 'a lwt_deserializer = lwtic -> 'a Lwt.t

type 'a lwt_pickler = 'a lwt_serializer * 'a lwt_deserializer

val bool_to  : bool   serializer
val char_to  : char   serializer
val int_to   : int    serializer
val int32_to : int32  serializer
val int64_to : int64  serializer
val float_to : float  serializer
val string_to: string serializer
val option_to: 'a serializer -> 'a option serializer
val string_option_to: string option serializer
val named_field_to: namedValue serializer
val list_to  : 'a serializer -> 'a list serializer
val string_list_to : string list serializer
val pair_to : 'a serializer -> 'b serializer -> ('a * 'b) serializer
val hashtbl_to: 'a serializer -> 'b serializer -> ('a, 'b) Hashtbl.t serializer


val bool_from : bool  deserializer
val char_from : char  deserializer
val int_from  : int   deserializer
val int32_from: int32 deserializer
val int64_from: int64 deserializer
val float_from: float deserializer
val string_from: string deserializer
val option_from: 'a deserializer -> 'a option deserializer
val string_option_from: string option deserializer
val list_from: 'a deserializer -> 'a list deserializer
val string_list_from: string list deserializer
val named_field_from: namedValue deserializer

val hashtbl_from: ('a * 'b) deserializer -> ('a, 'b) Hashtbl.t deserializer

val output_bool:   bool  lwt_serializer
val output_int:    int   lwt_serializer
val output_int32:  int32 lwt_serializer
val output_int64:  int64 lwt_serializer
val output_string_option: string option lwt_serializer
val output_string:        string lwt_serializer
val output_option:        'a lwt_serializer -> 'a option lwt_serializer
val output_key :          Key.t lwt_serializer
val output_pair : 'a lwt_serializer -> 'b lwt_serializer -> ('a * 'b) lwt_serializer
val output_counted_list:  'a lwt_serializer -> 'a counted_list lwt_serializer
val output_list:          'a lwt_serializer -> 'a list lwt_serializer

val output_string_list:   string list lwt_serializer
val output_hashtbl:
  (lwtoc -> 'a -> 'b -> unit Lwt.t) ->
  lwtoc -> ('a, 'b) Hashtbl.t -> unit Lwt.t

val output_kv_list: lwtoc -> ((string*string) list) -> unit Lwt.t
val output_string_pair : lwtoc -> (string * string) -> unit Lwt.t
val output_key_value_pair : lwtoc -> (Key.t * string) -> unit Lwt.t
val output_array : (lwtoc -> 'a -> unit Lwt.t) ->
  lwtoc -> 'a array -> unit Lwt.t
val output_array_reversed : (lwtoc -> 'a -> unit Lwt.t) -> lwtoc ->
  'a array -> unit Lwt.t
val output_string_array_reversed: lwtoc -> string array -> unit Lwt.t
val input_bool: lwtic   -> bool Lwt.t
val input_int:  lwtic   -> int Lwt.t
val input_int32: lwtic  -> int32 Lwt.t
val input_int64: lwtic  -> int64 Lwt.t
val input_string: lwtic -> string Lwt.t
val input_option: (lwtic -> 'a Lwt.t) -> lwtic -> ('a option) Lwt.t
val input_string_option: lwtic -> (string option) Lwt.t
val input_string_pair: lwtic -> (string * string) Lwt.t
val input_list  :(lwtic -> 'a Lwt.t) -> lwtic -> 'a list Lwt.t
val input_listl :(lwtic -> 'a Lwt.t) -> lwtic -> (int * 'a list) Lwt.t
val input_string_list: lwtic -> string list Lwt.t
val input_kv_list: lwtic -> ((string * string) list) Lwt.t
val input_hashtbl: (lwtic-> 'a Lwt.t) -> (lwtic -> 'b Lwt.t) ->
  lwtic -> ('a,'b) Hashtbl.t Lwt.t

val copy_stream:  length:int64 -> ic:lwtic -> oc:lwtoc -> unit Lwt.t
