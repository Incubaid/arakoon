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


open Tlogwriter
open Tlogcommon
open Update
open Tlogreader2
open Lwt


class type tlog_collection = object
  method validate_last_tlog: unit -> (tlogValidity * Entry.t option * Index.index) Lwt.t
  method iterate: Sn.t -> Sn.t -> (Entry.t -> unit Lwt.t) -> unit Lwt.t
  method log_value : Sn.t -> Value.t -> unit Lwt.t
  method log_value_explicit : Sn.t -> Value.t -> bool -> string option -> unit Lwt.t
  method get_last_i: unit -> Sn.t
  method get_last_value: Sn.t -> Value.t option (* Lwt.t *)
  method get_last: unit -> (Value.t * Sn.t) option
  method close : ?wait_for_compression : bool -> unit -> unit Lwt.t
  method get_infimum_i : unit -> Sn.t Lwt.t
  method dump_head : Lwt_io.output_channel -> Sn.t Lwt.t
  method save_head : Lwt_io.input_channel -> unit Lwt.t
  method dump_tlog_file : Sn.t -> Lwt_io.output_channel -> Sn.t Lwt.t
  method save_tlog_file : string -> int64 -> Lwt_io.input_channel -> unit Lwt.t
  method get_head_name : unit -> string
  method get_tlog_from_name : string -> Sn.t
  method get_tlog_from_i : Sn.t -> Sn.t
  method get_tlog_count: unit -> int Lwt.t
  method remove_oldest_tlogs : int -> unit Lwt.t
  method remove_below : Sn.t -> unit Lwt.t
  method which_tlog_file : Sn.t -> (string option) Lwt.t
end
