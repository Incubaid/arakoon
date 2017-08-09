(*
  Copyright (C) iNuron - info@openvstorage.com
  For license information, see <LICENSE.txt>
*)


open OUnit
open Value
open Mp_msg
open Message
open Update

let size_test_0 () =
  let v = create_client_value [] false in
  let p = MPMessage.Accept (0L, 0L, v) in
  let generic = MPMessage.generic_of p in
  let buff = Buffer.create 20 in
  let () = Message.to_buffer generic buff in
  let serialized = Buffer.contents buff in
  let size = String.length serialized in
  (* If ever, this test starts failing,
     re-evaluate the heuristic in
   *)
  let msg = "please re-evaluate the heuristic in" ^
              "multi_paxos_fsm's use of harvest_limited"
  in
  OUnit.assert_equal 40 size ~printer:string_of_int ~msg

let size_test_1 () =
  let u0 = Update.Set ("____00000012", "0123456789") in
  let size_u0 = Update.serialized_size u0 in
  let () = OUnit.assert_equal 34 size_u0 ~printer:string_of_int in
  let us = [u0] in
  let v = create_client_value us false in
  let p = MPMessage.Accept (0L, 0L, v) in
  let generic = MPMessage.generic_of p in
  let buff = Buffer.create 20 in
  let () = Message.to_buffer generic buff in
  let serialized = Buffer.contents buff in
  let size = String.length serialized in
  OUnit.assert_equal (40+size_u0) size ~printer:string_of_int

let suite =
  "mp_msg" >::: [
      "size_0" >:: size_test_0;
      "size_1" >:: size_test_1;
    ]
