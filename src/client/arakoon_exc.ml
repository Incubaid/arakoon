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




type rc =
  | E_OK
  | E_NO_MAGIC
  | E_NO_HELLO
  | E_NOT_MASTER
  | E_NOT_FOUND
  | E_WRONG_CLUSTER
  | E_ASSERTION_FAILED
  | E_READ_ONLY
  | E_OUTSIDE_INTERVAL
  | E_GOING_DOWN
  | E_NOT_SUPPORTED
  | E_NO_LONGER_MASTER
  | E_BAD_INPUT
  | E_INCONSISTENT_READ
  | E_USERFUNCTION_FAILURE
  | E_MAX_CONNECTIONS
  | E_UNKNOWN_FAILURE

let mapping = [
  ( E_OK                  , 0x00l);
  ( E_NO_MAGIC            , 0x01l);
  ( E_NO_HELLO            , 0x03l);
  ( E_NOT_MASTER          , 0x04l);
  ( E_NOT_FOUND           , 0x05l);
  ( E_WRONG_CLUSTER       , 0x06l);
  ( E_ASSERTION_FAILED    , 0x07l);
  ( E_READ_ONLY           , 0x08l);
  ( E_OUTSIDE_INTERVAL    , 0x09l);
  ( E_GOING_DOWN          , 0x10l);
  ( E_NOT_SUPPORTED       , 0x20l);
  ( E_NO_LONGER_MASTER    , 0x21l);
  ( E_BAD_INPUT           , 0x26l);
  ( E_INCONSISTENT_READ   , 0x80l);
  ( E_USERFUNCTION_FAILURE, 0x81l);
  ( E_MAX_CONNECTIONS     , 0xfel);
  ( E_UNKNOWN_FAILURE     , 0xffl );
]

let create f =
  let h = Hashtbl.create 47 in
  let () = List.iter
             (fun (a,b) -> let (a',b') = f (a,b) in Hashtbl.add h a' b')
             mapping
  in
  h

let hmap = create (fun x -> x)
let rmap = create (fun (a,b) -> (b,a))

let int32_of_rc rc = Hashtbl.find hmap rc


let rc_of_int32 i32 =
  try Hashtbl.find rmap i32 with Not_found -> E_UNKNOWN_FAILURE

exception Exception of rc * string

open Lwt

let output_exception oc rc msg =
  Llio.output_int32 oc (int32_of_rc rc) >>= fun () ->
  Llio.output_string oc msg
