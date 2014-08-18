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

open Tlogcommon
open Tlogreader2

class type tlog_collection = object
  method validate_last_tlog: unit -> (tlogValidity * Entry.t option * Index.index) Lwt.t
  method iterate: Sn.t -> Sn.t -> (Entry.t -> unit Lwt.t) -> unit Lwt.t
  method log_value : Sn.t -> Value.t -> unit Lwt.t
  method log_value_explicit : Sn.t -> Value.t -> ?validate:bool -> bool -> string option -> unit Lwt.t
  method get_last_i: unit -> Sn.t
  method get_last_value: Sn.t -> Value.t option (* Lwt.t *)
  method get_last: unit -> (Value.t * Sn.t) option
  method get_previous_checksum: Sn.t -> Checksum.Crc32.t option
  method set_previous_checksum: Checksum.Crc32.t option -> unit
  method close : ?wait_for_compression : bool -> unit -> unit Lwt.t
  method get_infimum_i : unit -> Sn.t Lwt.t
  method dump_head : Lwt_io.output_channel -> Sn.t Lwt.t
  method save_head : Lwt_io.input_channel -> unit Lwt.t
  method dump_tlog_file : Sn.t -> Lwt_io.output_channel -> Sn.t Lwt.t
  method save_tlog_file : ?validate:bool -> string -> int64 -> Lwt_io.input_channel -> unit Lwt.t
  method get_head_name : unit -> string
  method get_tlog_from_i : Sn.t -> Sn.t
  method get_tlog_count: unit -> int Lwt.t
  method remove_oldest_tlogs : int -> unit Lwt.t
  method remove_below : Sn.t -> unit Lwt.t
  method which_tlog_file : Sn.t -> (string option) Lwt.t
end
