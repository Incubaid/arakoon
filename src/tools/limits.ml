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



type resource = (* natural order, ie: CPU = 0 etc. *)
  | CPU
  | SIZE
  | EATA
  | STACK
  | CORE
  | SSS
  | PROC
  | NOFILE
  | MEMLOCK
  | AS
  | LOCKS
  | SIGPENDING
  | MSGQUEUE
  | NICE
  | RTPRIO
  | NLIMITS

type soft_or_hard =
  | Soft
  | Hard

external get_rlimit: resource -> soft_or_hard -> int = "arakoon_get_rlimit"

external get_maxrss: unit -> int  = "arakoon_get_maxrss"
