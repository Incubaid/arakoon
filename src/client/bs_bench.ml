open Unix
open Arg
open Otc

let clock f = 
  let t0 = Unix.gettimeofday () in
  let () = f() in
  let t1 = Unix.gettimeofday () in
  t1 -. t0

let make_key i = Printf.sprintf "key_%08i" i 

let sync = Bdb._dbsync

let set_loop db vs n = 
  let v = String.make vs 'x' in
  let set k v = Bdb.put db k v in
  let rec loop i = 
    if i = n 
    then sync db
    else
      let key = make_key i in
      let () = set key v in
      loop (i+1)
  in
  loop 0

let get_loop db n = 
  let get k = Bdb.get db k in
  let rec loop i =
    if i = n 
    then ()
    else
      let key = make_key i in
      let _ = get key in
      loop (i+1)
  in
  loop 0

let delete_loop db n = 
  let delete k = Bdb.out db k in
  let rec loop i = 
    if i = n 
    then sync db
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
  let db = Bdb._make () in
  let () = Bdb._dbopen db !fn Bdb.default_mode in
  let () = Printf.printf "\niterations = %i\nvalue_size = %i\n%!" !n !vs in
  let d = clock (fun () -> set_loop db !vs !n) in
  Printf.printf "%i sets: %fs\n%!" !n d;
  let d2 = clock (fun () -> get_loop db !n) in
  Printf.printf "%i gets: %fs\n%!" !n d2;
  let d3 = clock (fun () -> delete_loop db !n) in
  Printf.printf "%i deletes: %fs\n%!" !n d3;
  let () = Bdb._dbclose db in
  ();;
