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

module type ChecksumType = sig
  type t

  val string_of : t -> string
  val checksum_to : Buffer.t -> t -> unit
  val checksum_from : Llio.buffer -> t

  val calculate : string -> t
  val update : t -> string -> t
end

module Make (Cs : ChecksumType) = struct
  include Cs

  let update cs s =
    match cs with
    | None -> calculate s
    | Some cs -> Cs.update cs s
end

module Crc32Digest : ChecksumType = struct
  type t = int32

  let string_of = Printf.sprintf "%lx"
  let checksum_to = Llio.int32_to
  let checksum_from = Llio.int32_from

  let calculate s = Crc32c.calculate_crc32c s 0 (String.length s)
  let update cs s = Crc32c.update_crc32c cs s 0 (String.length s)
end


module Crc32 = Make (Crc32Digest)
