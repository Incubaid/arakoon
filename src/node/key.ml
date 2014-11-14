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

module Key = struct
  type t = string
  let make = id
  let length t = String.length t - 1
  let sub t start length = String.sub t (start + 1) length
  let to_oc t oc = Lwt_io.write_from_exactly oc t 1 (length t)
  let get t = sub t 0 (length t)
  let get_raw t = t
  let compare k1 k2 =
    (* both keys should have the same first character... *)
    String.compare k1 k2
end

include Key

module C = CompareLib.Default(Key)

include C
