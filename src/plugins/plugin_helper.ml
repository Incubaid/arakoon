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

let serialize_string = Llio.string_to
let serialize_hashtbl = Llio.hashtbl_to
let serialize_string_list = Llio.string_list_to

type input = Llio.buffer
let make_input = Llio.make_buffer

let deserialize_string = Llio.string_from
let deserialize_string_list = Llio.string_list_from
let deserialize_hashtbl = Llio.hashtbl_from

let generic_f f x =
  let k s = Lwt.ignore_result (f s) in
  Printf.kprintf k x

let debug_f x = generic_f Client_log.debug x
let info_f x = generic_f Client_log.info x
let warning_f x = generic_f Client_log.warning x
