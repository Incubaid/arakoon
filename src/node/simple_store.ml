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
exception Break

class transaction_lock = object end
class transaction = object end

type key = string
type value = string

module type Cursor_store = sig
  type cursor
  val cur_last : cursor -> bool
  val cur_get : cursor -> (key * value)
  val cur_get_key : cursor -> key
  val cur_get_value : cursor -> value
  val cur_prev : cursor -> bool
  val cur_next : cursor -> bool
  val cur_jump : cursor -> key -> bool (* jumps to the specified key or just right from it *)
end

module Extended_cursor_store(C : Cursor_store) = struct

  let cur_jump' cur key ~inc ~right =
    if C.cur_jump cur key
    then
      begin
        match inc, right with
        | true, true -> true
        | false, true ->
           let k = C.cur_get_key cur in
           if String.(=:) k key
           then
             C.cur_next cur
           else
             true
        | true, false ->
           let k = C.cur_get_key cur in
           if String.(=:) k key
           then
             true
           else
             C.cur_prev cur
        | false, false ->
           C.cur_prev cur
      end
    else
      if right
      then
        false
      else
        C.cur_last cur

  let _fold_range cur jump_init comp_end_key cur_next max f init =
    let rec inner acc count =
      if count = max
      then
        count, acc
      else
        begin
          let k = C.cur_get_key cur in
          if comp_end_key k
          then
            begin
              let count' = count + 1 in
              let acc' = f cur k count acc in
              if cur_next cur
              then
                inner acc' count'
              else
                count', acc'
            end
          else
            count, acc
        end
    in
    if jump_init
    then
      inner init 0
    else
      0, init

  let fold_range cur first finc last linc max f init =
    let comp_last =
      match last with
      | None ->
         fun k -> true
      | Some last ->
         if linc
         then
           fun k -> String.(<=:) k last
         else
           fun k -> String.(<:) k last in
    let jump_init = cur_jump' cur first ~inc:finc ~right:true in
    _fold_range cur jump_init comp_last C.cur_next max f init

  let fold_rev_range cur high hinc low linc max f init =
    let comp_low =
      if linc
      then
        fun k -> String.(>=:) k low
      else
        fun k -> String.(>:) k low in
    let jump_init = match high with
      | None ->
         C.cur_last cur
      | Some h ->
         cur_jump' cur h ~inc:hinc ~right:false in
    _fold_range cur jump_init comp_low C.cur_prev max f init

end

module type Simple_store = sig
  type t

  include Cursor_store
  val with_cursor : t -> (cursor -> 'a) -> 'a
  val with_transaction: t -> (transaction -> 'a Lwt.t) -> 'a Lwt.t

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
