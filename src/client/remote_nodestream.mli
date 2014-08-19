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

open Routing
open Interval
open Client_cfg
open Ncfg

class type nodestream = object
  method iterate:
    Sn.t -> f_entry:(Sn.t * Value.t -> unit Lwt.t) ->
    f_head:(Lwt_io.input_channel -> unit Lwt.t) ->
    f_file:(string -> int64 -> Lwt_io.input_channel -> unit Lwt.t) -> unit Lwt.t

  method collapse: int -> unit Lwt.t

  method set_routing: Routing.t -> unit Lwt.t
  method set_routing_delta: string -> string -> string -> unit Lwt.t
  method get_routing: unit -> Routing.t Lwt.t

  method optimize_db: unit -> unit Lwt.t
  method defrag_db:unit -> unit Lwt.t
  method get_db: string -> unit Lwt.t

  method get_fringe: string option -> Routing.range_direction -> ((string * string) list) Lwt.t
  method set_interval : Interval.t -> unit Lwt.t
  method get_interval : unit -> Interval.t Lwt.t

  method store_cluster_cfg: string -> ClientCfg.t -> unit Lwt.t

  method get_nursery_cfg: unit -> NCFG.t Lwt.t

  method drop_master: unit -> unit Lwt.t

end

val make_remote_nodestream :
  ?skip_prologue : bool -> string ->
  Lwt_io.input_channel * Lwt_io.output_channel -> nodestream Lwt.t
