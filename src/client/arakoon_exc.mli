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



type rc =
  | E_OK
  | E_NO_MAGIC
  | E_NO_HELLO
  | E_NOT_MASTER
  | E_NOT_FOUND
  | E_WRONG_CLUSTER
  | E_ASSERTION_FAILED
  | E_READ_ONLY
  | E_OUTSIDE_INTERVAL
  | E_GOING_DOWN
  | E_NOT_SUPPORTED
  | E_NO_LONGER_MASTER
  | E_BAD_INPUT
  | E_INCONSISTENT_READ
  | E_MAX_CONNECTIONS
  | E_UNKNOWN_FAILURE

val int32_of_rc : rc -> int32

val rc_of_int32: int32 -> rc

exception Exception of rc * string

val output_exception : Lwt_io.output_channel -> rc -> string -> unit Lwt.t
