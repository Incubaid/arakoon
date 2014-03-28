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

open OUnit
open Routing

let test_serialization () =
  let repr =[("left"),"k";],"right" in
  let routing = Routing.build repr in
  Printf.eprintf "original=%s\n" (Routing.to_s routing);
  let b = Buffer.create 127 in
  Routing.routing_to b routing ;
  let flat = Buffer.contents b in
  let inflated  = Routing.routing_from (Llio.make_buffer flat 0) in
  Printf.eprintf "reinflated=%s\n" (Routing.to_s inflated);
  OUnit.assert_equal routing inflated ~printer:Routing.to_s;
  ()

let test_change1 () =
  let repr = ["left","k";], "right" in
  let r = Routing.build repr in
  let r2 = Routing.change r "left" "m" "right" in
  let () = Printf.eprintf "r2=%s\n" (Routing.to_s r2) in
  let n = Routing.find r2 "l" in
  OUnit.assert_equal ~printer:(fun s -> s) n "left"

let test_change2 () =
  let repr = ["one","d";
              "two","k";
              "three","t";
             ], "four" in
  let r  = Routing.build repr in
  let r2 = Routing.change r "three" "s" "four" in
  let () = Printf.eprintf "r =%s\n" (Routing.to_s r) in
  let () = Printf.eprintf "r2=%s\n" (Routing.to_s r2) in
  let n = Routing.find r2 "s1" in
  OUnit.assert_equal ~printer:(fun s -> s) n "four"

let suite = "routing" >::: [
    "serialization" >:: test_serialization;
    "change1" >:: test_change1;
    "change2" >:: test_change2;
  ]
