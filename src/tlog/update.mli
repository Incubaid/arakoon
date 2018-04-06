(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE>
*)

val section : Lwt_log_core.section

module Range_assertion :
  sig
    type t = ContainsExactly of string list
    val to_string : t -> string
    val to_buffer : Buffer.t -> t -> unit
    val from_buffer : Llio.buffer -> t
    val serialized_size : t -> int
  end

module Update :
  sig
    type t =
        Set of string * string
      | Delete of string
      | MasterSet of string * float
      | TestAndSet of string * string option * string option
      | Sequence of t list
      | SetInterval of Arakoon_interval.Interval.t
      | SetRouting of Routing.Routing.t
      | SetRoutingDelta of (string * string * string)
      | Nop
      | Assert of string * string option
      | Assert_exists of string
      | Assert_range of string * Range_assertion.t
      | UserFunction of string * string option
      | AdminSet of string * string option
      | SyncedSequence of t list
      | DeletePrefix of string
      | Replace of string * string option
    val make_master_set : string -> float option -> t

    val update2s : ?values:bool -> t -> string
    val to_buffer : Buffer.t -> t -> unit
    val from_buffer : Llio.buffer -> t
    val is_synced : t -> bool
    val serialized_size : t -> int
  end
