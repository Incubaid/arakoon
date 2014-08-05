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

module Checksum : sig
  type t

  val zero : t

  val string_of : t -> string
  val of_string : string -> t
  val checksum_to : Buffer.t -> t -> unit
  val checksum_from : Llio.buffer -> t

  val calculate : string -> t
  val update : t -> string -> t
end = struct
  type t = int32

  let zero = Int32.zero

  let string_of cs = Printf.sprintf "%x" (Int32.to_int cs)
  let of_string s = Int32.of_int @@ Scanf.sscanf s "%x" (fun x -> x)
  let checksum_to = Llio.int32_to
  let checksum_from = Llio.int32_from

  let calculate _ = zero
  let update cs _ = Int32.succ cs
end

include Checksum
