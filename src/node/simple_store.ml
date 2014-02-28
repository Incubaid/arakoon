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

open Routing

let section = Logger.Section.main

let __i_key = "*i"
let __j_key = "*j"
let __interval_key = "*interval"
let __routing_key = "*routing"
let __master_key  = "*master"
let __lease_key = "*lease"
let __lease_key2 = "*lease2"
let __prefix = "@"
let __adminprefix="*"

type update_result =
  | Ok of string option
  | Update_fail of Arakoon_exc.rc * string

exception Key_not_found of string

class transaction_lock = object end
class transaction = object end

type key = string
type value = string

module type Simple_store = sig
  type t
  val with_transaction: t -> (transaction -> 'a Lwt.t) -> 'a Lwt.t

  type cursor
  val with_cursor : t -> (cursor -> 'a) -> 'a
  val cur_last : cursor -> bool
  val cur_get : cursor -> (key * value)
  val cur_get_key : cursor -> key
  val cur_prev : cursor -> bool
  val cur_next : cursor -> bool
  val cur_jump : cursor -> key -> bool (* jumps to the specified key or just right from it *)

  val exists: t -> string -> bool
  val get: t -> string -> string

  val set: t -> transaction -> string -> string -> unit
  val delete: t -> transaction -> string -> unit
  val delete_prefix: t -> transaction -> string -> int

  val flush: t -> unit Lwt.t
  val close: t -> bool -> unit Lwt.t
  val reopen: t -> (unit -> unit Lwt.t) -> bool -> unit Lwt.t
  val make_store: lcnum:int -> ncnum:int -> bool -> string -> t Lwt.t

  val get_location: t -> string
  val relocate: t -> string -> unit Lwt.t

  val get_key_count : t -> int64 Lwt.t

  val optimize : t -> bool -> unit Lwt.t
  val defrag : t -> unit Lwt.t
  val copy_store : t -> bool -> Lwt_io.output_channel -> unit Lwt.t
  val copy_store2 : string -> string -> bool -> unit Lwt.t
  val get_fringe : t -> string option -> Routing.range_direction -> (string * string) list Lwt.t
end

let _f _pf = function
  | Some x -> (Some (_pf ^ x))
  | None -> (Some _pf)
let _l _pf = function
  | Some x -> (Some (_pf ^ x))
  | None -> None

let _filter get_key make_entry fold coll pf =
  let pl = String.length pf in
  fold (fun acc entry ->
      let key = get_key entry in
      let kl = String.length key in
      let key' = String.sub key pl (kl-pl) in
      (make_entry entry key')::acc) [] coll

let filter_keys_list keys =
  _filter (fun i -> i) (fun e k -> k) List.fold_left keys __prefix

let filter_entries_list entries =
  _filter (fun (k,v) -> k) (fun (k,v) k' -> (k',v)) List.fold_left entries __prefix

let filter_keys_array entries =
  _filter (fun i -> i) (fun e k -> k) Array.fold_left entries __prefix

let next_prefix prefix =
  let next_char c =
    let code = Char.code c + 1 in
    match code with
      | 256 -> Char.chr 0, true
      | code -> Char.chr code, false in
  let rec inner s pos =
    let c, carry = next_char s.[pos] in
    s.[pos] <- c;
    match carry, pos with
      | false, _ -> Some s
      | true, 0 -> None
      | true, pos -> inner s (pos - 1) in
  let copy = String.copy prefix in
  inner copy ((String.length copy) - 1)
