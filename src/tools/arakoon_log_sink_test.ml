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

let test_log_sink () =
  let sinks =
    let open Arakoon_log_sink in
    [ File (Unix.getcwd () ^ "/x"), "x";
      File "/tmp", "/tmp";
      File "/tmp", "file:///tmp";
      File (Unix.getcwd () ^ "/x/a"), "x/a";
      File "/tmp/a", "/tmp/a";
      File "/tmp/a", "file:///tmp/a";
      Redis ("localhost", 628, "/key1"), "redis://localhost:628/key1";
      Redis ("127.0.0.1", 6379, "//key2"), "redis://127.0.0.1//key2";
      Console, "console:";
    ]
  in
  List.iter
    (fun (expected, url) ->
     let actual = Arakoon_log_sink.make url in
     Printf.printf "url = %s, expected = %s, actual = %s\n"
                   url
                   (Arakoon_log_sink.to_string expected)
                   (Arakoon_log_sink.to_string actual);
     assert (actual = expected))
    sinks

open OUnit
let suite = "arakoon_log_sink" >::: [
      "test_log_sink" >:: test_log_sink;
    ]
