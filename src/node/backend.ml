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
open Update
open Arakoon_interval
open Routing
open Statistics
open Client_cfg
open Arakoon_client

class type backend = object
  inherit Registry.backend

  method exists: consistency:consistency -> string -> bool
  method get: consistency:consistency -> string -> string
  method set: string -> string -> unit Lwt.t
  method confirm: string -> string -> unit Lwt.t
  method aSSert: consistency:consistency -> string -> string option -> unit
  method aSSert_exists: consistency:consistency -> string -> unit
  method delete: string -> unit Lwt.t
  method test_and_set: string -> string option -> string option ->
                       (string option) Lwt.t
  method replace: string -> string option -> (string option) Lwt.t
  method range:
    consistency:consistency ->
    string option -> bool ->
    string option -> bool -> int -> (Key.t array)
  method range_entries:
    consistency:consistency ->
    string option -> bool ->
    string option -> bool -> int -> ((Key.t * string) counted_list)
  method rev_range_entries:
    consistency:consistency ->
    string option -> bool ->
    string option -> bool -> int -> ((Key.t * string) counted_list)
  method prefix_keys:
    consistency:consistency -> string -> int -> (Key.t counted_list)
  method last_entries : Sn.t ->Lwt_io.output_channel -> unit Lwt.t
  method last_entries2: Sn.t ->Lwt_io.output_channel -> unit Lwt.t
  method last_entries3: Sn.t ->Lwt_io.output_channel -> unit Lwt.t

  method multi_get:
    consistency:consistency ->
    string list -> string list

  method multi_get_option:
    consistency:consistency ->
    string list -> string option list

  method hello: string -> string -> string Lwt.t
  method flush_store : unit -> unit Lwt.t

  method who_master: unit -> string option
  method sequence: sync:bool -> Update.t list -> unit Lwt.t

  method witness : string -> Sn.t -> unit

  method last_witnessed : string -> Sn.t

  method expect_progress_possible : unit -> bool

  method get_statistics: unit -> Statistics.t
  method clear_most_statistics: unit -> unit
  method check: cluster_id:string -> bool

  method collapse : int -> (int -> unit Lwt.t) -> (unit -> unit Lwt.t) -> unit Lwt.t

  method user_function: string -> string option -> (string option) Lwt.t

  method read_allowed : consistency -> unit
  method get_read_user_db : unit -> Registry.read_user_db

  method set_interval : Interval.t -> unit Lwt.t
  method get_interval : unit -> Interval.t
  method get_routing: unit -> Routing.t
  method set_routing: Routing.t -> unit Lwt.t
  method set_routing_delta: string -> string -> string -> unit Lwt.t

  method get_key_count: unit -> int64

  method get_db: Lwt_io.output_channel option -> unit Lwt.t
  method optimize_db: unit -> unit Lwt.t
  method defrag_db:unit -> unit Lwt.t
  method copy_db_to_head : int -> unit Lwt.t

  method get_fringe: string option -> Routing.range_direction -> ((Key.t * string) counted_list)

  method get_cluster_cfgs: unit -> (string, ClientCfg.t) Hashtbl.t
  method set_cluster_cfg: string -> ClientCfg.t -> unit Lwt.t

  method delete_prefix: string -> int Lwt.t

  method drop_master: unit -> unit Lwt.t
  method get_current_state : unit -> string
  method nop : unit -> unit Lwt.t
  method get_txid: unit -> consistency
end
