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
open Crc32c

let rawData = [| 0x01; 0xC0; 0x00; 0x00; 0x00; 0x00; 0x00; 0x00;
                 0x00; 0x00; 0x00; 0x00; 0x00; 0x00; 0x00; 0x00; 0x01; 0xFE; 0x60;
                 0xAC; 0x00; 0x00; 0x00; 0x08; 0x00; 0x00; 0x00; 0x04; 0x00; 0x00;
                 0x00; 0x09; 0x25; 0x00; 0x00; 0x00; 0x00; 0x00; 0x00; 0x00; 0x00;
                 0x00; 0x00; 0x00; 0x00; 0x00; 0x00; 0x00 |]

let expected = 0x664f75ebl

let test_correctness () =
  let len = Array.length rawData in
  let rawDataC = Array.map char_of_int rawData in
  let buf = Bytes.create len in
  let _ = Array.iteri (fun i c -> Bytes.set buf i c) rawDataC in
  let res = calculate_crc32c buf 0 len in
  let msg = Printf.sprintf "correctness expected: %lx real %lx\n" expected res in
  OUnit.assert_equal ~msg res expected

let test_correctness5 () =
  let len = Array.length rawData in
  let rawDataC = Array.map char_of_int rawData in
  let buf  = Bytes.create len in
  let ()   = Array.iteri (fun i c -> Bytes.set buf i c) rawDataC in
  let buf1 = String.sub buf 0 12 in
  let res  = calculate_crc32c buf1 0 12 in
  let buf2 = String.sub buf 12 12 in
  let res2 = update_crc32c res buf2 0 12 in
  let buf3 = String.sub buf 24 24 in
  let res3 = update_crc32c res2 buf3 0 24 in
  let msg = Printf.sprintf "correctness5 expected: %lx real %lx\n" expected res3 in
  OUnit.assert_equal ~msg res3 expected

let test_odd_sizes () =
  let len = Array.length rawData in
  let rawDataC = Array.map char_of_int rawData in
  let buf  = Bytes.create len in
  let ()   = Array.iteri (fun i c -> Bytes.set buf i c) rawDataC in
  let rec loop n =
    if n = 0 then ()
    else
      let buf2 = String.sub buf 0 n in
      let _res  = calculate_crc32c buf2 0 n in
      loop (n-1)
  in
  loop len

let suite =
  "CRC32c" >::: [
    "correctness" >:: test_correctness;
    "correctness5" >:: test_correctness5;
    "odd_sizes" >:: test_odd_sizes;
  ]
