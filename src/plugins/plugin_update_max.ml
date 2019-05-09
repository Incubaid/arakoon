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


open Arakoon_registry

let s2i = int_of_string
let i2s = string_of_int

let update_max db po =
  let _k = "max" in
  let v =
    match db # get _k with
      | None -> 0
      | Some s -> s2i s
  in
  let v' = match po with
    | None -> 0
    | Some s ->
      try s2i s
      with _ -> raise (Arakoon_exc.Exception(Arakoon_exc.E_UNKNOWN_FAILURE, "invalid arg"))
  in
  let m = max v v' in
  let ms = i2s m in
  db#put _k (Some ms);
  Some (i2s m)




let () = Registry.register "update_max" update_max
