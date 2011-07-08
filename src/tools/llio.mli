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

val lwt_failfmt :  ('a, unit, string, 'b Lwt.t) format4 -> 'a

val bool_to  : Buffer.t -> bool   -> unit
val int_to   : Buffer.t -> int    -> unit
val int32_to : Buffer.t -> int32  -> unit
val int64_to : Buffer.t -> int64  -> unit
val float_to : Buffer.t -> float  -> unit
val string_to: Buffer.t -> string -> unit
val option_to: (Buffer.t -> 'a -> unit) -> Buffer.t -> 'a option -> unit
val string_option_to: Buffer.t -> string option -> unit

val bool_from : string -> int -> bool  * int
val int_from  : string -> int -> int   * int
val int32_from: string -> int -> int32 * int
val int64_from: string -> int -> int64 * int
val float_from: string -> int -> float * int
val string_from: string -> int -> string * int
val option_from: (string -> int -> 'a * int) -> string -> int -> 'a option * int
val string_option_from: string -> int -> string option * int

val output_bool: Lwt_io.output_channel -> bool   -> unit Lwt.t
val output_int: Lwt_io.output_channel -> int     -> unit Lwt.t
val output_int32: Lwt_io.output_channel -> int32 -> unit Lwt.t
val output_int64: Lwt_io.output_channel -> int64 -> unit Lwt.t
val output_string_option: Lwt_io.output_channel  -> string option -> unit Lwt.t
val output_string: Lwt_io.output_channel -> string -> unit Lwt.t
val output_list: 
  (Lwt_io.output_channel -> 'a -> unit Lwt.t) ->
  Lwt_io.output_channel -> 'a list -> unit Lwt.t

val output_string_pair : Lwt_io.output_channel -> (string * string) -> unit Lwt.t

val input_bool: Lwt_io.input_channel -> bool Lwt.t
val input_int: Lwt_io.input_channel -> int Lwt.t
val input_int32: Lwt_io.input_channel -> int32 Lwt.t
val input_int64: Lwt_io.input_channel -> int64 Lwt.t
val input_string: Lwt_io.input_channel -> string Lwt.t
val input_string_option: Lwt_io.input_channel -> (string option) Lwt.t
val input_string_pair: Lwt_io.input_channel -> (string * string) Lwt.t
val input_list:(Lwt_io.input_channel -> 'a Lwt.t) -> Lwt_io.input_channel -> 'a list Lwt.t


val copy_stream:  
  length:int64 -> 
  ic:Lwt_io.input_channel -> 
  oc:Lwt_io.output_channel 
  -> unit Lwt.t
