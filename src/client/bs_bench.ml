open Unix
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
    then sync t
    else
      let key = make_key i in
      let () = set key v in
      loop (i+1)
  in
  loop 0


let _get_option db key = try Some (Bdb.get db key) with Not_found -> None 

let _test_and_set db key expected wanted = 
  let r = 
    let existing = _get_option db key in
    if existing = expected
    then
      begin
        match wanted with
        | None ->
           begin
             match existing with
             | None -> ()
             | Some _ -> Bdb.out db key
           end
        | Some wanted_s -> Bdb.put db key wanted_s
      end;
    existing
  in
  Lwt.return r


let _test_and_set2 db key expected wanted = 
  Bdb.with_cursor 
    db 
    (fun bdb cursor -> 
     Bdb.jump bdb cursor key;
     let k = Bdb.key bdb cursor in
     let existing = 
       if k = key 
       then Some (Bdb.value bdb cursor) 
       else None 
     in
     if existing = expected
     then
       begin
         match wanted with
         | None ->
             begin
               match existing with 
               | None -> () 
               | Some _ -> Bdb.cur_out bdb cursor
             end
          | Some wanted_s -> Bdb.cur_put bdb cursor wanted_s 0 (* 0 = CURRENT *)
        end;
      existing)
    
let tas_loop t tas vs n = 
  let db = Hotc.get_bdb t in
  let vw = Some (String.make vs 'x') in
  let ve = Some (String.make vs 'y') in
  let rec loop i = 
    if i = n 
    then sync t
    else
      let r = Random.int n in
      let key = make_key r in
      let _ = _test_and_set db key ve vw in
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
  let rec loop i =
    if i = n 
    then Lwt.return ()
    else
      let r = Random.int n in
      let key = make_key r in
      let _ = _get_option db key in
      loop (i+1)
  in
  loop 0

let get_random_loop2 t n = 
  let db = Hotc.get_bdb t in
  let get_batch keys = List.map (_get_option db) (List.sort String.compare keys) in
  let rec loop (batch,size) i = 
    if i = n 
    then
      let _ = get_batch batch in
      Lwt.return ()
    else
      if size = 100 
      then
        let _ = get_batch batch in
        loop ([],0) (i+1)
      else
        let r = Random.int n in
        let k = make_key r in
        loop (k::batch,size+1) (i+1)
  in
  loop ([],0) 0

let delete_loop t n = 
  let db = Hotc.get_bdb t in
  let delete k = Bdb.out db k in
  let rec loop i = 
    if i = n 
    then sync t
    else
      let key = make_key i in
      let () = delete key in
      loop (i+1)
  in
  loop 0


let () = 
  let n  = ref 1_000_000 in
  let vs = ref 2_000 in
  let lcnum = ref 1024 in
  let ncnum = ref 512 in
  let fn = ref "test.db" in
  let () = 
    Arg.parse [
      ("--value-size",Set_int vs, Printf.sprintf "size of the values in bytes (%i)" !vs);
      ("--file", Set_string fn, Printf.sprintf "file name for database (%S)" !fn);
      ("--bench-size",Set_int n,  Printf.sprintf "number of sets/gets/deletes (%i)" !n);
      ("--lcnum", Set_int lcnum,  Printf.sprintf "leaf cache size (%i);" !lcnum);
      ("--ncnum", Set_int ncnum,  Printf.sprintf "node cache size (%i);" !ncnum);
    ]
      (fun _ ->()) 
      "simple baardskeerder like benchmark for tc"
  in
  let t () = 
    Hotc.create !fn ~lcnum:!lcnum ~ncnum:!ncnum [] >>= fun ho ->
    let () = Printf.printf "\niterations = %i\nvalue_size = %i\n%!" !n !vs in
    clock (fun () -> set_loop ho !vs !n) >>= fun d ->
    Lwt_io.printlf "%i sets: %fs%!" !n d >>= fun () ->

    clock (fun () -> get_loop ho !n) >>= fun d2 ->
    Lwt_io.printlf "%i ordered gets: %fs%!" !n d2 >>= fun () ->

    clock (fun () -> get_random_loop ho !n) >>= fun d4 ->
    Lwt_io.printlf "%i random gets: %fs%!" !n d4 >>= fun () ->

    clock (fun () -> get_random_loop2 ho !n) >>= fun d6 ->
    Lwt_io.printlf "%i random gets2: %fs%!" !n d6  >>= fun () ->
    clock (fun () -> tas_loop ho _test_and_set !vs !n) >>= fun d5 ->
    Lwt_io.printlf "%i random tas: %fs%!" !n d5 >>= fun () ->
    
    clock (fun () -> delete_loop ho !n) >>= fun d3 ->
    Lwt_io.printlf "%i deletes: %fs%!" !n d3 >>= fun () ->
    
    Hotc.close ho >>= fun () ->
    Lwt.return ()
  in
  Lwt_main.run (t ())
 
