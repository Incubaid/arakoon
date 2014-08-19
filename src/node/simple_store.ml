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

let section = Logger.Section.main

let next_prefix prefix =
  if 0 = String.length prefix
  then
    None
  else
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

let __i_key = "*i"
let __j_key = "*j"
let __interval_key = "*interval"
let __routing_key = "*routing"
let __master_key  = "*master"
let __checksum_key = "*checksum"
(* let __lease_key = "*lease" *)
(* let __lease_key2 = "*lease2" *)
let __prefix = "@"
let __prefix_successor =
  match next_prefix __prefix with
  | Some s -> s
  | None -> failwith "impossible"
let __adminprefix="*"

let make_public k =
  __prefix ^ k

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
  val cur_jump : cursor -> ?direction:direction -> key -> bool
end

let _fold_range cur jump_init comp_end_key cur_get_key cur_next max f init =
  let rec inner acc count =
    if count = max
    then
      count, acc
    else
      begin
        let k = cur_get_key cur in
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

module Extended_cursor_store(C : Cursor_store) = struct

  let cur_jump' cur key ~inc ~right =
    if C.cur_jump cur ~direction:(if right then Right else Left) key
    then
      begin
        if inc
        then
          true
        else
          (* maybe move 1 position left or right *)
          begin
            let k = C.cur_get_key cur in
            if String.(=:) k key
            then
              begin
                if right
                then
                  C.cur_next cur
                else
                  C.cur_prev cur
              end
            else
              true
          end
      end
    else
      false

  let fold_range cur first finc last linc max f init =
    let comp_last =
      match last with
      | None -> fun _ -> true
      | Some last ->
         if linc
         then
           fun k -> String.(<=:) k last
         else
           fun k -> String.(<:) k last in
    let jump_init = cur_jump' cur first ~inc:finc ~right:true in
    _fold_range cur jump_init comp_last C.cur_get_key C.cur_next max f init

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
    _fold_range cur jump_init comp_low C.cur_get_key C.cur_prev max f init

end

module type Simple_store = sig
  type t

  include Cursor_store
  val with_cursor : t -> (cursor -> 'a) -> 'a
  val with_transaction: t -> (transaction -> 'a Lwt.t) -> 'a Lwt.t

  val exists: t -> string -> bool
  val get: t -> string -> string

  val set: t -> transaction -> string -> string -> unit
  val put: t -> transaction -> string -> string option -> unit
  val delete_prefix: t -> transaction -> string -> int

  (* special case this one, for speed. *)
  val range : t ->
              string -> bool -> string option -> bool ->
              int -> string array

  val flush: t -> unit Lwt.t
  val close: t -> flush:bool -> sync:bool -> unit Lwt.t
  val reopen: t -> (unit -> unit Lwt.t) -> bool -> unit Lwt.t
  val make_store: lcnum:int -> ncnum:int -> bool -> string -> t Lwt.t

  val get_location: t -> string
  val relocate: t -> string -> unit Lwt.t

  val get_key_count : t -> int64

  val optimize : t -> quiesced:bool -> stop:bool ref -> bool Lwt.t
  val defrag : t -> unit Lwt.t
  val copy_store : t -> bool -> Lwt_io.output_channel -> unit Lwt.t
  val copy_store2 : string -> string ->
                    overwrite:bool ->
                    throttling:float ->
                    unit Lwt.t
end

let _f _pf = function
  | Some x -> _pf ^ x
  | None -> _pf

let _l _pf = function
  | Some x -> (Some (_pf ^ x))
  | None -> None
