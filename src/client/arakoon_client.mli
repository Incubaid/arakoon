(** main interface to arakoon *)
open Statistics

type key = string
type value = string

type change =
  | Set of key * value
  | Delete of key
  | TestAndSet of key * value option * value option
  | Sequence of change list

class type client = object

  (** Tests presence of a value for particular key *)
  method exists: key -> bool Lwt.t

  (** retrieves the value for a key *)
  method get: key -> value Lwt.t
  (** or fails with Arakoon_exc.Exception (E_NOT_FOUND,_) if there is none *)

    
  method set: key -> value -> unit Lwt.t

  method delete: key -> unit Lwt.t

    
  method range: key option -> bool -> key option -> bool -> int -> key list Lwt.t

  (** will yield a list of key value pairs in range. *)
  method range_entries:
    first:key option -> finc:bool ->
    last:key option ->  linc:bool -> max:int ->
    (key * value) list Lwt.t
  (** [range_entries ~first ~finc ~last ~linc ~max] 
      [max] is the maximum number of keys (if max < 0 then you want them all).
      The keys fall in the range first..last. 
      The booleans [finc] [linc] determine iff the boundaries are considered
      in the result
  *)


  (** yields the list of keys starting with that prefix *)
  method prefix_keys: key -> int -> key list Lwt.t

  (** updates a value conditionally *)
  method test_and_set: key -> value option -> value option -> (value option) Lwt.t
  (** 
      [test_and_set key expected wanted] updates the value 
      for that key only of it matches the expected value.
      It always yields the value for that key after the operation. 
      Note that wanted can be None and can be used to delete a value
  *)

  method multi_get: key list -> (value list) Lwt.t
  
  method hello: string -> string Lwt.t

  method sequence: change list -> unit Lwt.t (* ... hm ... *)

  method who_master: unit -> string option Lwt.t

  method expect_progress_possible: unit -> bool Lwt.t

  method statistics: unit -> Statistics.t Lwt.t
end
