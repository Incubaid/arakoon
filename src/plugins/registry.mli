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
    method get : string -> string
    method with_cursor : (cursor_db -> 'a) -> 'a
  end

class type user_db =
  object
    inherit read_user_db
    method set : string -> string -> unit
    method delete: string -> unit
    method test_and_set: string -> string option -> string option -> string option
  end


module Registry : sig
  type f = user_db -> string option -> string option
  val register : string -> f -> unit
  val lookup : string -> f
end


module HookRegistry : sig
  type continuation =
    | Return of string
    | Update of string * string option
  type h = read_user_db -> string -> continuation
  val register : string -> h -> unit
  val lookup : string -> h
end
