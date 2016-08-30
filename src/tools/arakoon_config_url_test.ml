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

open Arakoon_config_url

let test_parse_url () =
  let urls = [
      Etcd (["127.0.0.1", 5000], "/albas/xxxx//arakoons/abm"), "etcd://127.0.0.1:5000/albas/xxxx//arakoons/abm";
      File "/tmp", "/tmp";
      File "/tmp", "file:///tmp";
      File (Unix.getcwd () ^ "/x/a"), "x/a";
      Arakoon ({ cluster_id = "id";
                 key = "some/key";
                 ini_location = "/tmp/x";
               }),
      "arakoon://id/some/key?ini=/tmp/x";
    ]
  in
  List.iter
    (fun (expected, url) ->
     let actual = make url in
     Printf.printf "url = %s, expected = %s, actual = %s\n"
                   url
                   (to_string expected)
                   (to_string actual);
     assert (expected = actual))
    urls

open OUnit
let suite = "arakoon_config_url" >::: [
      "test_parse_url" >:: test_parse_url;
    ]
