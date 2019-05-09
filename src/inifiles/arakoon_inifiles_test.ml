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

let _parse txt =
  try
    let _cfg = Node_cfg.Node_cfg._retrieve_cfg_from_txt txt in ()
  with (Arakoon_inifiles.Ini_parse_error(lnum,txt) as exn)->
    let () =
      Printf.printf "lnum:%i\n%s" lnum txt in
    raise exn

let test_parsing1 () =
  let txt =
    let lines =[
      "[global]"
      ; "cluster = abm_0, abm_1, abm_2\n"
      ; "cluster_id = abm"
      ; "plugins = albamgr_plugin nsm_host_plugin"
      ; "\n"

      ; "[abm_0]"
      ; "ip = 127.0.0.1"
      ; "client_port = 4000"
      ; "messaging_port = 4010"
      ; "home = /home/romain/workspace/tmp/arakoon/abm/abm_0"
      ; "log_level = debug"
      ; "fsync = false"
      ; "\n"

      ; "[abm_1]"
      ; "ip = 127.0.0.1"
      ; "client_port = 4001"
      ; "messaging_port = 4011"
      ; "home = /home/romain/workspace/tmp/arakoon/abm/abm_1"
      ; "log_level = debug"
      ; "fsync = false"
      ; "\n"

      ;"[abm_2]";"ip = 127.0.0.1"
      ;"client_port = 4002"
      ;"messaging_port = 4012"
      ;"home = /home/romain/workspace/tmp/arakoon/abm/abm_2"
      ;"log_level = debug"
      ;"fsync = false"
      ]
    in
    String.concat "\n" lines
  in
  _parse txt

let test_parsing2 () =
  let txt =
    Lwt_extra.run
      (fun () ->
       Lwt_extra.read_file "./cfg/arakoon.ini"
      )
  in
  _parse txt

open OUnit
let suite = "inifiles" >:::[
      "parsing1" >:: test_parsing1;
      "parsing2" >:: test_parsing2;
    ]
