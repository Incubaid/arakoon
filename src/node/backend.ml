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

open Update
open Statistics

class type backend = object
  method exists: allow_dirty:bool -> string -> bool Lwt.t
  method get: allow_dirty:bool -> string -> string Lwt.t
  method set: string -> string -> unit Lwt.t
  method delete: string -> unit Lwt.t
  method test_and_set: string -> string option -> string option -> (string option) Lwt.t
  method range:
    allow_dirty:bool ->
      string option -> bool ->
	string option -> bool -> int -> (string list) Lwt.t
  method range_entries:
    allow_dirty:bool ->
      string option -> bool ->
	string option -> bool -> int -> ((string * string) list) Lwt.t
  method prefix_keys: 
    allow_dirty:bool ->
    string -> int -> (string list) Lwt.t
  method last_entries: Sn.t ->Lwt_io.output_channel -> unit Lwt.t

  method multi_get: 
    allow_dirty:bool ->
      string list -> string list Lwt.t
  method hello: string -> string -> (int32 * string) Lwt.t

  method who_master: unit -> string option Lwt.t
  method sequence: Update.t list -> unit Lwt.t

  method witness : string -> Sn.t -> unit Lwt.t

  method expect_progress_possible : unit -> bool Lwt.t
    
  method get_statistics: unit -> Statistics.t

  method check: cluster_id:string -> bool Lwt.t

  method collapse : int -> (int -> unit Lwt.t) -> (unit -> unit Lwt.t) -> unit Lwt.t
end
