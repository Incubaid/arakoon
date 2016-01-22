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

type url =
  File of string
  | Etcd of (((string * int) list) * string)

let to_string = function
  | File f -> f
  | Etcd (peers,path) ->
     let peers_s =
       List.map
         (fun (host,port) -> Printf.sprintf "%s:%i" host port)
         peers
       |> String.concat ","
     in
     Printf.sprintf "etcd://%s/%s" peers_s path

let make string =
  let protocol, rest =
    try
      Scanf.sscanf string "%s@://%s" (fun p r -> p,r)
    with End_of_file ->
      "file", string
  in
  match protocol with
  | "file" ->
      let canonical =
        if rest.[0] = '/'
        then rest
        else Filename.concat (Unix.getcwd()) rest
      in
      File canonical
  | "etcd" ->
     let host,port,path =
       Scanf.sscanf rest "%s@:%i/%s" (fun h i p -> h,i,p)
     in
     let peers = [host,port]
     in
     Etcd (peers, path)
  | _ -> failwith (Printf.sprintf "unknown protocol:%s" protocol)
