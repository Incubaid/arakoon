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

open Lwt
open OUnit
open Tlogcommon

let test_old_format () =
  let do_it () =
    let fn = "./data/005.tlc" in
    let print_entry a entry =
      let i = Entry.i_of entry in
      let v = Entry.v_of entry in
      let is = Sn.string_of i in
      let vs = Value.value2s v in
      Lwt_io.eprintlf "%s:%s" is vs >>= fun () ->
      Lwt.return a
    in
    let f ic =
      let a0 = () in
      let first = Sn.of_int 500015 in
      let start = Sn.of_int 500015 in
      let stop  = Some (Sn.of_int 500175) in
      Tlogreader2.O.fold ic ~index:None
        start ~first stop a0 print_entry >>= fun () ->
      Lwt.return ()
    in
    Lwt_io.with_file ~mode:Lwt_io.input fn f
  in
  Lwt_main.run (do_it ())


let suite = "tlogreader2" >::: ["old_format" >:: test_old_format]
