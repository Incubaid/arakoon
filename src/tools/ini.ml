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



let id (x : 'a) : 'a = x

let p_string s = Scanf.sscanf s "%s" id

let p_int s = Scanf.sscanf s "%i" id

let p_float s = Scanf.sscanf s "%f" id

let p_string_list s = Str.split (Str.regexp "[, \t]+") s

let p_bool s = Scanf.sscanf s "%b" id

let p_option p s = Some (p s)

let required section name = failwith  (Printf.sprintf "missing: %s %s" section name)

let default x = fun _ _ -> x

let get inifile section name s2a not_found =
  try
    let v_s = inifile # getval section name in
    s2a v_s
  with (Inifiles.Invalid_element _) -> not_found section name
