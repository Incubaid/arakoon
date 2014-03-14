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

val bool_to  : Buffer.t -> bool   -> unit
val char_to  : Buffer.t -> char   -> unit
val int_to   : Buffer.t -> int    -> unit
val int32_to : Buffer.t -> int32  -> unit
val int64_to : Buffer.t -> int64  -> unit
val float_to : Buffer.t -> float  -> unit
val string_to: Buffer.t -> string -> unit
val option_to: (Buffer.t -> 'a -> unit) -> Buffer.t -> 'a option -> unit
val string_option_to: Buffer.t -> string option -> unit
val named_field_to: Buffer.t -> namedValue -> unit
val list_to  : Buffer.t -> (Buffer.t -> 'a -> unit) -> 'a list -> unit
val string_list_to : Buffer.t -> string list -> unit
val hashtbl_to: Buffer.t -> (Buffer.t -> 'a -> 'b -> unit) ->
  ('a, 'b) Hashtbl.t -> unit

type buffer
val make_buffer : string -> int -> buffer
val buffer_pos : buffer -> int
val buffer_done : buffer -> bool
val buffer_set_pos: buffer -> int -> unit

val bool_from : buffer  -> bool
val char_from : buffer  -> char
val int_from  : buffer  -> int
val int32_from: buffer  -> int32
val int64_from: buffer  -> int64
val float_from: buffer  -> float
val string_from: buffer -> string
val option_from: (buffer -> 'a ) -> buffer -> 'a option
val string_option_from: buffer -> string option
val list_from: buffer -> (buffer -> 'a ) -> 'a list
val string_list_from: buffer -> string list
val named_field_from: buffer -> namedValue

val hashtbl_from: buffer -> (buffer -> 'a * 'b) -> ('a, 'b) Hashtbl.t

val output_bool:          lwtoc -> bool          -> unit Lwt.t
val output_int:           lwtoc -> int           -> unit Lwt.t
val output_int32:         lwtoc -> int32         -> unit Lwt.t
val output_int64:         lwtoc -> int64         -> unit Lwt.t
val output_string_option: lwtoc -> string option -> unit Lwt.t
val output_string:        lwtoc -> string        -> unit Lwt.t
val output_key :          lwtoc -> Key.t         -> unit Lwt.t
val output_counted_list:
  (lwtoc -> 'a -> unit Lwt.t) ->
  lwtoc -> 'a counted_list -> unit Lwt.t
val output_list:
  (lwtoc -> 'a -> unit Lwt.t) ->
  lwtoc -> 'a list -> unit Lwt.t

val output_string_list:   lwtoc -> string list   -> unit Lwt.t
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
val input_string_option: lwtic -> (string option) Lwt.t
val input_string_pair: lwtic -> (string * string) Lwt.t
val input_list  :(lwtic -> 'a Lwt.t) -> lwtic -> 'a list Lwt.t
val input_listl :(lwtic -> 'a Lwt.t) -> lwtic -> (int * 'a list) Lwt.t
val input_string_list: lwtic -> string list Lwt.t
val input_kv_list: lwtic -> ((string * string) list) Lwt.t
val input_hashtbl: (lwtic-> 'a Lwt.t) -> (lwtic -> 'b Lwt.t) ->
  lwtic -> ('a,'b) Hashtbl.t Lwt.t

val copy_stream:  length:int64 -> ic:lwtic -> oc:lwtoc -> unit Lwt.t
