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

let test_compatibility_int64() =
  let do_one v =
    let s = String.create 8 in
    let () = Cllio.int64_to_buffer s 0 v in
    let v' = Llio.int64_from (Llio.make_buffer s 0) in
    Printf.eprintf "%Li:%S\n" v s;
    assert_equal ~msg: "phase1" v v';
    ()
      (*
  let b = Buffer.create 8 in
  let () = LLio.int64_to b v in
  let bs = Buffer.contents b in
  let v' = ...
      *)
  in
  let vs = [0L;1L;-1L;65535L;49152L] in
  List.iter do_one vs

let test_compatibility_int32_to() =
  let do_one i32 =
    let b = Buffer.create 16 in
    let () = Llio.int32_to b i32 in
    let s = Buffer.contents b in
    let i32' = Cllio.int32_from s 0 in
    assert_equal ~printer:Int32.to_string i32 i32';
    ()
  in
  let i32s = [0l;1l;-1l;65535l;49152l;65537l]in
  List.iter do_one i32s


let suite =
  "Cllio" >:::[
    "compatibility_int64" >:: test_compatibility_int64;
    "compatibility_int32_to" >:: test_compatibility_int32_to;
  ]
