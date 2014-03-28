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



let option2s f = function
  | None -> "None"
  | Some v -> "Some (\"" ^ String.escaped (f v ) ^ "\")"


let string_option2s = option2s (fun s -> s)
let int_option2s = option2s string_of_int
let p_option = string_option2s

let list2s e_to_s list =
  let inner =
    List.fold_left (fun acc a -> acc ^ (e_to_s a) ^ ";") "" list
  in "[" ^ inner ^ "]"

let log_o o x =
  let k s =
    let os = o # to_string () in
    Client_log.debug  (os ^": " ^  s)
  in
  Printf.ksprintf k x
