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
  val of_string : string -> t
  val checksum_to : Buffer.t -> t -> unit
  val checksum_from : Llio.buffer -> t

  val calculate : string -> t
  val update : t -> string -> t
end

module type C = sig
  include ChecksumType

  val none : t
  val is_none : t -> bool
end

module Make (Cs : ChecksumType) : C = struct
  type t = Cs.t option

  let none = None

  let is_none = function
    | None -> true
    | Some _ -> false

  let string_of = function
    | None -> "_"
    | Some cs -> Cs.string_of cs

  let of_string = function
    | "_" -> None
    | s -> Some (Cs.of_string s)

  let checksum_to buf = function
    | Some cs -> Cs.checksum_to buf cs
    | None -> failwith "should not happen"

  let checksum_from buf = Some (Cs.checksum_from buf)

  let calculate s = Some (Cs.calculate s)

  let update cs s =
    match cs with
    | None -> calculate s
    | Some cs -> Some (Cs.update cs s)
end

module Crc32 : ChecksumType = struct
  type t = int32

  let string_of = Printf.sprintf "%lx"
  let of_string s = Scanf.sscanf s "%lx" (fun x -> x)
  let checksum_to = Llio.int32_to
  let checksum_from = Llio.int32_from

  let calculate s = Crc32c.calculate_crc32c s 0 (String.length s)
  let update cs s = Crc32c.update_crc32c cs s 0 (String.length s)
end

module Md5 : ChecksumType = struct
  type t = Digest.t

  let string_of = Digest.to_hex
  let of_string = Digest.from_hex
  let checksum_to = Llio.string_to
  let checksum_from = Llio.string_from

  let calculate s = Digest.string s
  let update cs s = Digest.string (cs ^ Digest.string s)
end

include Make (Crc32)
