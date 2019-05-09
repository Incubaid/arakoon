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



open Client_cfg
open OUnit

let test_parsing () =
  let open Lwt.Infix in
  let t () =
    let lines = [
        "[global]";
        "cluster_id = sturdy";
        "cluster = sturdy_0,sturdy_1, sturdy_2  ";
        "\n";
        "[sturdy_0]";
        "client_port = 7080";
        "name = sturdy_0";
        "ip = 127.0.0.1";
        "\n";
        "[sturdy_1]";
        "client_port = 7081";
        "name = sturdy_1";
        "ip = 127.0.0.1";
        "\n";
        "[sturdy_2]";
        "ip = 127.0.0.1";
        "client_port = 7082";
        "name = sturdy_2"]
    in
    let contents = List.fold_left (fun acc v -> acc ^ "\n" ^ v) "" lines in
    let fn = "/tmp/client_cfg_test.ml" in
    Lwt_io.with_file
      ~mode:Lwt_io.Output fn
      (fun oc -> Lwt_io.write oc contents)
    >>= fun () ->
    Arakoon_config_url.(retrieve (File fn)) >|= (ClientCfg._from_txt "global") >>= fun cfg ->
    let sa0 = ClientCfg.get cfg "sturdy_0" in
    let r = OUnit.assert_equal sa0 (["127.0.0.1"],7080) in
    Lwt.return r
  in
  Lwt_extra.run t

let suite = "client_cfg" >:::[
    "parsing" >:: test_parsing;
  ]
