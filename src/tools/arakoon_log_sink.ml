(*
Copyright 2016 iNuron NV

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

type t =
  | Redis of string * int * string
  | File of string
  | Console

let to_string = function
  | Redis (host, port, key) -> Printf.sprintf "Redis(%s,%i,%s)" host port key
  | File file -> Printf.sprintf "File(%s)" file
  | Console -> "Console"

let make s =
  let uri = Uri.of_string s in
  let path = Uri.path uri in
  match Uri.scheme uri with
  | None
  | Some "file" ->
     let canonical =
       if path.[0] = '/'
       then path
       else Filename.concat (Unix.getcwd()) path
     in
     File canonical
  | Some "redis" ->
     let host = match Uri.host uri with
       | None -> failwith "host is required"
       | Some h -> h
     in
     let port = match Uri.port uri with
       | None -> 6379
       | Some p -> p
     in
     Redis (host, port, path)
  | Some "console" ->
     Console
  | Some protocol ->
     failwith (Printf.sprintf "unknown protocol:%s" protocol)
