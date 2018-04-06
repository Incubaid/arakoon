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

(* open and user plugin_helper so that it's available for plugins! *)
open Plugin_helper
let () = ignore (make_input "" 0)

exception UserFunctionNotFound of string

class type cursor_db =
  object
    method get_key : unit -> Key.t
    method get_value : unit -> string
    method jump : ?inc:bool -> ?right:bool -> string -> bool
    method last : unit -> bool
    method next : unit -> bool
    method prev : unit -> bool
  end

module Cursor_store = struct
  let fold_range (cur : cursor_db) first finc last linc max f init =
    let jump_init =
      cur # jump first ~inc:finc in
    let comp_last =
      match last with
      | None ->
         fun _ -> true
      | Some last ->
         let last_key = Key.make (Simple_store.make_public last) in
         if linc
         then fun k -> Key.(k <=: last_key)
         else fun k -> Key.(k <: last_key) in
    Simple_store._fold_range
      cur
      jump_init
      comp_last
      (fun cur -> cur # get_key ())
      (fun cur -> cur # next ())
      max
      f
      init

  let fold_rev_range (cur : cursor_db) high hinc low linc max f init =
    let comp_low =
      let low_key = Key.make (Simple_store.make_public low) in
      if linc
      then fun k -> Key.(k >=: low_key)
      else fun k -> Key.(k >: low_key) in
    let jump_init = match high with
      | None ->
         cur # last ()
      | Some k ->
         cur # jump k ~inc:hinc ~right:false in
    Simple_store._fold_range
      cur
      jump_init
      comp_low
      (fun cur -> cur # get_key ())
      (fun cur -> cur # prev ())
      max
      f
      init
end

class type read_user_db =
  object
    method exists : string -> bool
    method get : string -> string option
    method get_exn : string -> string
    method with_cursor : (cursor_db -> 'a) -> 'a

    method get_interval : unit -> Arakoon_interval.Interval.t
  end

class type user_db =
  object
    inherit read_user_db
    method put : string -> string option -> unit
  end

class type backend =
  object
    method read_allowed : Arakoon_client.consistency -> unit
    method push_update : Update.Update.t -> string option Lwt.t
  end

module Registry = struct
  type f = user_db -> string option -> string option
  let _r = Hashtbl.create 42
  let register name (f:f) = Hashtbl.replace _r name f
  let exists name = Hashtbl.mem _r name
  let lookup name =
    try Hashtbl.find _r name
    with Not_found -> raise (UserFunctionNotFound name)
end

module HookRegistry = struct
  (* input and output channel *)
  type h = (Llio.lwtic * Llio.lwtoc * string) -> read_user_db -> backend -> unit Lwt.t
  let _r = Hashtbl.create 42
  let register name (h:h) = Hashtbl.replace _r name h
  let lookup name = Hashtbl.find _r name
end
