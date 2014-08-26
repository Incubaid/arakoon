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

open Arg
open Camltc
open Lwt

let clock f =
  let t0 = Unix.gettimeofday () in
  f() >>= fun () ->
  let t1 = Unix.gettimeofday () in
  Lwt.return (t1 -. t0)

let make_key i = Printf.sprintf "key_%08i" i

let sync t = Camltc.Hotc.sync t

let set_loop t vs n =
  let db = Hotc.get_bdb t in
  let v = String.make vs 'x' in
  let set k v = Bdb.put db k v in
  let rec loop i =
    if i = n
    then Lwt.return (sync t)
    else
      let key = make_key i in
      let () = set key v in
      loop (i+1)
  in
  loop 0

let get_loop t n =
  let db = Hotc.get_bdb t in
  let get k = Bdb.get db k in
  let rec loop i =
    if i = n
    then Lwt.return ()
    else
      let key = make_key i in
      let _ = get key in
      loop (i+1)
  in
  loop 0

let get_random_loop t n =
  let db = Hotc.get_bdb t in
  let get k = try Some (Bdb.get db k) with Not_found -> None in
  let rec loop i =
    if i = n
    then Lwt.return ()
    else
      let r = Random.int n in
      let key = make_key r in
      let _ = get key in
      loop (i+1)
  in
  loop 0

let delete_loop t n =
  let db = Hotc.get_bdb t in
  let delete k = Bdb.out db k in
  let rec loop i =
    if i = n
    then Lwt.return (sync t)
    else
      let key = make_key i in
      let () = delete key in
      loop (i+1)
  in
  loop 0

let () =
  let n  = ref 1_000_000 in
  let vs = ref 2_000 in
  let fn = ref "test.db" in
  let () =
    Arg.parse [
      ("--value-size",Set_int vs, Printf.sprintf "size of the values in bytes (%i)" !vs);
      ("--file", Set_string fn, Printf.sprintf "file name for database (%S)" !fn);
      ("--bench-size",Set_int n,  Printf.sprintf "number of sets/gets/deletes (%i)" !n);
    ]
      (fun _ ->())
      "simple baardskeerder like benchmark for tc"
  in
  let t () =
    Hotc.create !fn [] >>= fun ho ->
    let () = Printf.printf "\niterations = %i\nvalue_size = %i\n%!" !n !vs in
    clock (fun () -> set_loop ho !vs !n) >>= fun d ->
    Lwt_io.printlf "%i sets: %fs%!" !n d >>= fun () ->
    clock (fun () -> get_loop ho !n) >>= fun d2 ->
    Lwt_io.printlf "%i ordered gets: %fs%!" !n d2 >>= fun () ->
    clock (fun () -> get_random_loop ho !n) >>= fun d4 ->
    Lwt_io.printlf "%i random gets: %fs%!" !n d4 >>= fun () ->
    clock (fun () -> delete_loop ho !n) >>= fun d3 ->
    Lwt_io.printlf "%i deletes: %fs%!" !n d3 >>= fun () ->

    Hotc.close ho >>= fun () ->
    Lwt.return ()
  in
  Lwt_main.run (t ())
