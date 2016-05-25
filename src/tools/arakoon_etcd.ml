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

exception ProcessFailure of Unix.process_status

let verify_process_status p =
  p # status >>= function
  | Unix.WEXITED 0 -> Lwt.return_unit
  | s -> Lwt.fail (ProcessFailure s)

let retrieve_value peers path =
  (*
    curl -X GET "http://127.0.0.1.:5000/v2/keys/path_to/asd/055a61c0_0"
    or
    etcdctl --peers=127.0.0.1:5000 get path_to/asd/055a61c0_0
   *)
  let peers_s = String.concat "," (List.map (fun (h,p) -> Printf.sprintf "%s:%i" h p) peers) in
  let cmd = [| "etcdctl";
               "--peers=" ^ peers_s;
               "get";
               "--quorum";
               path;
              |]
  in
  Lwt_log.info_f  "ETCD: %s" (Array.to_list cmd |> String.concat " ") >>= fun () ->
  Lwt_process.with_process_in
    ("", cmd)
    (fun p ->
     Lwt_io.read (p # stdout) >>= fun value ->
     verify_process_status p >>= fun () ->
     (* etcdctl adds a '\n' after the value, cut it off *)
     String.sub value 0 (String.length value - 1) |> Lwt.return)

let store_value peers path value =
  let peers_s = String.concat "," (List.map (fun (h,p) -> Printf.sprintf "%s:%i" h p) peers) in
  let cmd = [| "etcdctl";
               "--peers=" ^ peers_s;
               "set";
               path;
               value;
              |]
  in
  Lwt_log.info_f  "ETCD: %s" (Array.to_list cmd |> String.concat " ") >>= fun () ->
  Lwt_process.with_process_none
    ("", cmd)
    (fun p -> verify_process_status p)
