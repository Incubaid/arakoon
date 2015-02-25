(*
 * Copyright (2010-2014) INCUBAID BVBA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *)

external id : 'a -> 'a = "%identity"

let int i = string_of_int i
let int64 i = Printf.sprintf "%LdL" i
let string s = Printf.sprintf "%S" s
let bool = function
  | true -> "true"
  | false -> "false"
let float f = string_of_float f

let option f = function
  | None -> "None"
  | Some v -> "Some " ^ f v

let list f l =
    let parts = List.map f l in
    let inner = String.concat "; " parts in
    Printf.sprintf "[%s]" inner

let record m =
    let parts = List.map (fun (k, v) -> Printf.sprintf "%s = %s" k v) m in
    let inner = String.concat "; " parts in
    Printf.sprintf "{ %s }" inner

let pair a b (va, vb) =
    Printf.sprintf "(%s, %s)" (a va) (b vb)
