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
  | Sequence of change list

class type client = object

  (** Tests presence of a value for particular key *)
  method exists: ?allow_dirty:bool -> key -> bool Lwt.t

  (** retrieves the value for a key *)
  method get: ?allow_dirty:bool -> key -> value Lwt.t
  (** or fails with Arakoon_exc.Exception (E_NOT_FOUND,_) if there is none *)

  method range: ?allow_dirty:bool -> key option -> bool -> key option -> bool -> int -> key list Lwt.t
  (** will yield a list of key value pairs in range. *)

method range_entries:
  ?allow_dirty:bool ->
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
  ?allow_dirty:bool ->
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
  method prefix_keys: ?allow_dirty:bool -> key -> int -> key list Lwt.t

  method multi_get: ?allow_dirty:bool -> key list -> (value list) Lwt.t

  (** retuns the list of value options for the keys *)
  method multi_get_option: ?allow_dirty:bool -> key list -> (value option list) Lwt.t

  method set: key -> value -> unit Lwt.t

  method confirm: key -> value -> unit Lwt.t
  (** [confirm key value] does nothing if this value was already
      associated to the key, otherwise, it behaves as [set key value]
  *)
  method aSSert: ?allow_dirty:bool -> key -> value option -> unit Lwt.t
  (**
     [aSSert key vo] throws Arakoon_exc.Exception (E_ASSERTION_FAILED,_) if
     the value associated with the key is not what was expected.
  *)
  method aSSert_exists: ?allow_dirty:bool -> key -> unit Lwt.t

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

  method ping: string -> string -> string Lwt.t

  method sequence: change list -> unit Lwt.t (* ... hm ... *)

  method synced_sequence : change list -> unit Lwt.t

  method who_master: unit -> string option Lwt.t

  method expect_progress_possible: unit -> bool Lwt.t

  method statistics: unit -> Statistics.t Lwt.t

  method user_function: string -> string option -> string option Lwt.t

  method get_key_count: unit -> int64 Lwt.t

  method get_cluster_cfgs: unit -> NCFG.t Lwt.t

  method version : unit -> (int * int * int * string) Lwt.t
    (** returns [(major,minor,patch, info)] *)

  method current_state : unit -> string Lwt.t
end
