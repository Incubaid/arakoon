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
open Lwt_buffer

let _ =
  let rec produce item period q =
    Lwt_io.printlf "produced: %s" item >>= fun () ->
    Lwt_buffer.add item q >>= fun () ->
    Lwt_unix.sleep period >>= fun () ->
    produce item period q
  in
  let capacity = Some 5 in
  let q0 = Lwt_buffer.create ~capacity () in
  let q1 = Lwt_buffer.create ~capacity () in
  let q2 = Lwt_buffer.create ~capacity () in
  let p0 () = produce "p0_stuff" 1.0 q0 in
  let p1 () = produce "p1_stuff" 1.0 q1 in
  let p2 () = produce "p2_stuff" 1.0 q2 in

  let rec c qs =
    let consume q  =
      Lwt_unix.sleep 3.0 >>= fun () ->
      Lwt_buffer.take q >>= fun item ->
      Lwt_io.printlf "consumed %S" item
    in
    let ready q = Lwt_buffer.wait_for_item q  >>= fun () -> Lwt.return q
    in
    let waiters = List.map ready qs in
    Lwt.npick waiters >>= fun ready_qs ->

    Lwt_list.iter_s consume ready_qs >>= fun () ->
    c qs


  in
  let qs = [q2;q1;q0] in
  let main () = join [p0();
                      p1();
                      p2();
                      c qs;
                     ]
  in
  Lwt_main.run (main());;
