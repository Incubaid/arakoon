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



type master =
  | Elected
  | ReadOnly
  | Forced of string
  | Preferred of string list

let master2s = function
  | Elected -> "Elected"
  | ReadOnly -> "ReadOnly"
  | Forced s -> Printf.sprintf "(Forced %s)" s
  | Preferred s -> Printf.sprintf "(Preferred [%s])" (String.concat ", " s)
