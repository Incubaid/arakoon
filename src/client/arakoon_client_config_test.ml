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

open Arakoon_client_config

(* TODO
- test
met quickcheck een config aanmaken
to string
parsen
assert equal

 *)

let test_to_from_ini () =
  let cluster_id = "x" in
  let tcp_keepalive =
    { enable_tcp_keepalive = true;
      tcp_keepalive_time = 9;
      tcp_keepalive_intvl = 90;
      tcp_keepalive_probes = 21; }
  in
  let node_cfgs = [ ("node1", { ips = [ "23"; "99fsdjv"; ]; port = 423; });
                    ("node2", { ips = []; port = 23; }); ]
  in
  let ssl_cfg = Some {
                    ca_cert = "ca_cert";
                    creds = Some ("x", "y");
                    protocol = Ssl.TLSv1;
                  } in
  let t = { cluster_id; tcp_keepalive; node_cfgs; ssl_cfg; } in
  let t' =
    let x = to_ini t |> from_ini in
    { x with node_cfgs = List.rev x.node_cfgs }
  in
  assert (t = t')

open OUnit

let suite =
  "arakoon_client_config" >::: [
      "test_to_from_ini" >:: test_to_from_ini;
    ]
