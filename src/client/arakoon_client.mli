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

(** main interface to arakoon *)
open Statistics
open Ncfg

type key = string
type value = string

type change =
  | Set of key * value
  | Delete of key
  | Assert of (key * value option)
  | Assert_exists of (key)
  | TestAndSet of key * value option * value option
  | UserFunction of string * string option
  | Sequence of change list

type consistency =
  | Consistent
  | No_guarantees
  | At_least of Stamp.t

class type client = object

  (** Tests presence of a value for particular key *)
  method exists: ?consistency:consistency -> key -> bool Lwt.t

  (** retrieves the value for a key *)
  method get: ?consistency:consistency -> key -> value Lwt.t
  (** or fails with Arakoon_exc.Exception (E_NOT_FOUND,_) if there is none *)

  method range: ?consistency:consistency -> key option -> bool -> key option -> bool -> int -> key list Lwt.t
  (** will yield a list of key value pairs in range. *)

  method range_entries:
    ?consistency:consistency ->
    first:key option -> finc:bool ->
    last:key option ->  linc:bool -> max:int ->
    (key * value) list Lwt.t
  (** [range_entries ~first ~finc ~last ~linc ~max]
      [max] is the maximum number of keys (if max < 0 then you want them all).
      The keys fall in the range first..last.
      The booleans [finc] [linc] determine iff the boundaries are considered
      in the result
  *)

  method rev_range_entries:
    ?consistency:consistency ->
    first:key option -> finc:bool ->
    last:key option ->  linc:bool -> max:int ->
    (key * value) list Lwt.t
  (** [rev_range_entries ~first ~finc ~last ~linc ~max]
      [max] is the maximum number of keys (if max < 0 then you want them all).
      The keys fall in the range first..last.
      The booleans [finc] [linc] determine iff the boundaries are considered
      in the result
  *)

  (** yields the list of keys starting with that prefix *)
  method prefix_keys: ?consistency:consistency -> key -> int -> key list Lwt.t

  method multi_get: ?consistency:consistency -> key list -> (value list) Lwt.t

  (** retuns the list of value options for the keys *)
  method multi_get_option: ?consistency:consistency -> key list -> (value option list) Lwt.t

  method set: key -> value -> unit Lwt.t

  method get_txid: unit -> consistency Lwt.t

  method nop : unit -> unit Lwt.t
  (** [nop ()] is a paxos no-operation.
  *)
  method confirm: key -> value -> unit Lwt.t
  (** [confirm key value] does nothing if this value was already
      associated to the key, otherwise, it behaves as [set key value]
  *)
  method aSSert: ?consistency:consistency -> key -> value option -> unit Lwt.t
  (**
     [aSSert key vo] throws Arakoon_exc.Exception (E_ASSERTION_FAILED,_) if
     the value associated with the key is not what was expected.
  *)
  method aSSert_exists: ?consistency:consistency -> key -> unit Lwt.t

  method delete: key -> unit Lwt.t

  method delete_prefix: key -> int Lwt.t
  (**
     [delete_prefix prefix] deletes all entries with the prefix and yields the number of entries deleted
  *)

  (** updates a value conditionally *)
  method test_and_set: key -> value option -> value option -> (value option) Lwt.t
  (**
      [test_and_set key expected wanted] updates the value
      for that key only of it matches the expected value.
      It always yields the value for that key after the operation.
      Note that wanted can be None and can be used to delete a value
  *)

  method replace : key -> value option -> (value option) Lwt.t
  (**
      [replace key wanted] assigns the wanted value to the key,
      and returns the previous assignment (if any) for that key.
      If wanted is None, the binding is deleted.
  *)

  method ping: string -> string -> string Lwt.t

  method sequence: change list -> unit Lwt.t (* ... hm ... *)

  method synced_sequence : change list -> unit Lwt.t

  method who_master: unit -> string option Lwt.t

  method expect_progress_possible: unit -> bool Lwt.t

  method statistics: unit -> Statistics.t Lwt.t

  method user_function: string -> string option -> string option Lwt.t
  method user_hook: ?consistency:consistency -> string -> (Llio.lwtic * Llio.lwtoc) Lwt.t

  method get_key_count: unit -> int64 Lwt.t

  method get_cluster_cfgs: unit -> NCFG.t Lwt.t

  method version : unit -> (int * int * int * string) Lwt.t
  (** returns [(major,minor,patch, info)] *)

  method current_state : unit -> string Lwt.t
end
