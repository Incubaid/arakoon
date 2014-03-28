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


class type cursor_db =
  object
    method get_key : unit -> Key.t
    method get_value : unit -> string
    method jump : string -> bool
    method last : unit -> bool
    method next : unit -> bool
    method prev : unit -> bool
  end

class type read_user_db =
  object
    method get : string -> string option
    method with_cursor : (cursor_db -> 'a) -> 'a
  end

class type user_db =
  object
    inherit read_user_db
    method put : string -> string option -> unit
  end

module Registry = struct
  type f = user_db -> string option -> string option (* TODO add routing here? *)
  let _r = Hashtbl.create 42
  let register name (f:f) = Hashtbl.replace _r name f
  let lookup name = Hashtbl.find _r name
end

module HookRegistry = struct
  type continuation =
    | Return of string (* TODO functional unparsing? *)
    | Update of string * string option (* user function name + payload *)
  type h = read_user_db -> string -> continuation (* TODO add routing here? *)
  let _r = Hashtbl.create 42
  let register name (h:h) = Hashtbl.replace _r name h
  let lookup name = Hashtbl.find _r name
end
