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

open Lwt.Infix

let parse_url url =
  let host,port,path =
    Scanf.sscanf url "%s@:%i/%s" (fun h i p -> h,i,p)
  in
  let peers = [host,port] in
  peers, path

let retrieve_value peers path =
  (*
    curl -X GET "http://127.0.0.1.:5000/v2/keys/path_to/asd/055a61c0_0"
    or
    etcdctl --peers=127.0.0.1:5000 get path_to/asd/055a61c0_0
   *)
  let peers_s = String.concat "," (List.map (fun (h,p) -> Printf.sprintf "%s:%i" h p) peers) in
  let cmd = ["etcdctl";
             "--peers=" ^ peers_s;
             "get";
             path
            ]
  in
  let cmd_s = String.concat " " cmd in
  Lwt_io.eprintlf "cmd_s: %s" cmd_s >>= fun () ->
  Lwt_log.info_f  "ETCD: %s" cmd_s >>= fun () ->
  let command = Lwt_process.shell cmd_s in
  Lwt_process.with_process_in
    command
    (fun p ->
     let stream = Lwt_io.read_lines (p # stdout) in
     Lwt_stream.to_list stream >>= fun lines ->
     let txt = String.concat "\n" lines in
     Lwt.return txt
    )

let retrieve_cfg (p:string -> 'a Lwt.t) = function
  | Arakoon_config_url.File cfg_file ->
     Lwt_extra.read_file cfg_file >>= p
  | Arakoon_config_url.Etcd (peers,path)->
     retrieve_value peers path >>= p
