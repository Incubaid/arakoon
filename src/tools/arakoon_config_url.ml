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


type arakoon_cfg =
  { cluster_id : string;
    key : string;
    ini_location : string }

type url =
  | File of string
  | Etcd of (((string * int) list) * string)
  | Arakoon of arakoon_cfg

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
  | Arakoon { cluster_id; key; ini_location; } ->
     Uri.make
       ~scheme:"arakoon"
       ~host:cluster_id
       ~path:key
       ~query:[ ("ini", [ ini_location; ]); ]
       ()
     |> Uri.to_string


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
  | Some "etcd" ->
     let host = match Uri.host uri with
       | None -> failwith "host is required"
       | Some h -> h
     in
     let port = match Uri.port uri with
       | None -> 6379
       | Some p -> p
     in
     let peers = [ host, port ] in
     Etcd (peers, path)
  | Some "arakoon" ->
     let path = Str.string_after path 1 in
     let cluster_id = match Uri.host uri with
       | None -> failwith "host/cluster_id is required"
       | Some h -> h
     in
     let qs = Uri.query uri in
     let ini_location =
       try List.assoc "ini" qs |> List.hd
       with Not_found ->
         failwith "ini location was not specified"
     in
     Arakoon { cluster_id; key = path; ini_location; }
  | Some protocol -> failwith (Printf.sprintf "unknown protocol:%s" protocol)

open Lwt.Infix

let retrieve = function
  | File f ->
     Lwt_extra.read_file f
  | Etcd (peers, path) ->
     Arakoon_etcd.retrieve_value peers path
  | Arakoon { cluster_id; key; ini_location; } ->
     Lwt_extra.read_file ini_location >>= fun txt ->
     let cfg = Arakoon_client_config.from_ini txt in
     assert (cluster_id = cfg.Arakoon_client_config.cluster_id);
     Client_helper.with_master_client'
       cfg
       (fun client -> client # get key)

let default_url = ref (make "cfg/arakoon.ini")
