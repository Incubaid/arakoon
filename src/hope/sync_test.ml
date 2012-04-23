open Lwt
open OUnit
open Bstore


let test_correctness() = 
  let make_key i = Printf.sprintf "key%i" i in
  let make_value i = Printf.sprintf "value%i" i in
  let fn0 = "/tmp/sync_test_store0.bs" in
  let setup () = 
    Lwt_io.printlf "test_correctness" >>= fun () ->
    BStore.init fn0 >>= fun () ->
    BStore.create fn0 >>= fun bstore ->
    let n = 10 in
    let rec loop i = 
      if i = n
      then Lwt.return () 
      else 
        let k = make_key i in
        let v = make_value i in
        let u = Core.SET (k,v) in
        BStore.log bstore true u >>= function
          | Core.UNIT    -> loop (i + 1)
          | Core.FAILURE (_,_) -> Lwtc.failfmt "xxx"
    in
    loop 0 >>= fun () ->
    Lwt.return (bstore,fn0)
  in
  let teardown bstore fn = BStore.close bstore in


  setup () >>= fun (bstore0,fn0) ->
  let fn1 = "/tmp/sync_test_store1.bs" in
  BStore.init fn1 >>= fun () ->
  BStore.create fn1 >>= fun bstore1 ->
  let buffer = "/tmp/sync_test_buffer.bin" in
  let a0 = Core.TICK 0L in
  Lwt_io.with_file 
    ~mode:Lwt_io.output 
    buffer
    (fun oc -> 
      BStore.last_entries bstore0 a0 oc>>= fun () ->
      Sn.output_sn oc (-1L)
    ) 

  >>= fun () ->
  Lwt_io.printlf "dumped last_entries" >>= fun () ->
  let f () i acs = 
    Lwt_io.printlf "f () (i:%Li)" i >>= fun () ->
    let us = List.map action2update acs in
    let u = Core.SEQUENCE us in
    let d = true in
    BStore.log  bstore1 d u >>= function 
      | Core.UNIT    -> Lwt.return () 
      | Core.FAILURE(_,_) -> Lwtc.failfmt "xxxx"
  in
  Lwt_io.with_file
    ~mode:Lwt_io.input
    buffer
    (fun ic -> Sync.iterate f () ic)
  >>= fun () ->
  (* heuristic verification of contents of bstore1 *)
  let k = make_key 1 in
  BStore.get bstore1 k >>= fun v ->
  Lwt_io.printlf "value=%s" v >>= fun () ->
  OUnit.assert_equal v (make_value 1);
  teardown bstore0 fn0 >>= fun () -> 
  teardown bstore1 fn1 >>= fun () ->
  Lwt.return ()




let () = 
  let t () = test_correctness () in
  Lwtc.configure_logging();
  Lwt_main.run (t ())
      
  
