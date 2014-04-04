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

open Unix
open Arg
open Lwt

module Bench(S : Simple_store.Simple_store) = struct

  let clock f =
    let t0 = Unix.gettimeofday () in
    f () >>= fun () ->
    let t1 = Unix.gettimeofday () in
    Lwt.return (t1 -. t0)

  let make_key i = Printf.sprintf "key_%08i" i

  let sync t = S.sync t

  let set_loop t vs n =
    let v = String.make vs 'x' in
    let set k v = S.with_transaction t (fun tx -> S.set t tx k v; Lwt.return ()) in
    let rec loop i =
      if i = n
      then sync t
      else
        let key = make_key i in
        set key v >>= fun () ->
        loop (i+1)
    in
    loop 0

  let get_loop t n =
    let get k = S.get t k in
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
    let get k = try Some (S.get t k) with Not_found -> None in
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
    let delete k = S.with_transaction t (fun tx -> S.delete t tx k; Lwt.return ()) in
    let rec loop i =
      if i = n
      then sync t
      else
        let key = make_key i in
        delete key >>= fun () ->
        loop (i+1)
    in
    loop 0

  let run_tests fn vs n =
    let t () =
      S.make_store
        ~lcnum:Node_cfg.default_lcnum
        ~ncnum:Node_cfg.default_ncnum
        false fn >>= fun s ->
      let () = Printf.printf "\niterations = %i\nvalue_size = %i\n%!" n vs in
      clock (fun () -> set_loop s vs n) >>= fun d ->
      Lwt_io.printlf "%i sets: %fs%!" n d >>= fun () ->
      clock (fun () -> get_loop s n) >>= fun d2 ->
      Lwt_io.printlf "%i ordered gets: %fs%!" n d2 >>= fun () ->
      clock (fun () -> get_random_loop s n) >>= fun d4 ->
      Lwt_io.printlf "%i random gets: %fs%!" n d4 >>= fun () ->
      clock (fun () -> delete_loop s n) >>= fun d3 ->
      Lwt_io.printlf "%i deletes: %fs%!" n d3 >>= fun () ->

      S.close s true >>= fun () ->
      Lwt.return ()
    in
    Lwt_main.run (t ())
end

let () =
  let n  = ref 1_000_000 in
  let vs = ref 2_000 in
  let fn = ref "test.db" in
  let store_type = ref "tc" in
  let () =
    Arg.parse
      [ ("--value-size",Set_int vs, Printf.sprintf "size of the values in bytes (%i)" !vs);
        ("--file", Set_string fn, Printf.sprintf "file name for database (%S)" !fn);
        ("--bench-size",Set_int n,  Printf.sprintf "number of sets/gets/deletes (%i)" !n);
        ("--store-type",Set_string store_type, "the type of store to test (either 'tc' or 'leveldb')"); ]
      (fun _ -> ())
      "simple baardskeerder like benchmark for simple store"
  in
  match !store_type with
  | "tc" ->
     let module S = (val (module Batched_store.Local_store : Simple_store.Simple_store)) in
     let module B = Bench(S) in
     B.run_tests !fn !vs !n
  | "leveldb" ->
     let module S = (val (module Leveldb_store.Level_store : Simple_store.Simple_store)) in
     let module B = Bench(S) in
     B.run_tests !fn !vs !n
  | _ ->
     failwith "invalid store type"
