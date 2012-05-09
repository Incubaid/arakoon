open OUnit
open Lwt
open Bstore
open Core

let lwt_wrap setup init f teardown =
  setup init >>= fun r ->
  Lwt.catch
    (fun () -> f r >>= fun () -> teardown true r)
    (fun e -> teardown false r >>= fun () -> Lwt.fail e)
  

let setup () =
  let fn = Filename.temp_file "t_log_" ".bs" in
  Lwt_io.printlf "fn=%s\n" fn >>= fun () ->
  BStore.init fn >>= fun () ->
  BStore.create fn >>= fun t ->
  Lwt.return (fn,t)

let teardown ok (fn,t) =
  BStore.close t >>= fun () ->
  if ok 
  then Lwt_unix.unlink fn
  else Lwt.return ()

  
let t_log () =  
  let test (fn,t) = 
    let u = SET("x","x") in
    BStore.log t true u >>= fun _ ->
    let u2 = SEQUENCE [ASSERT("x",Some "x")] in
    BStore.log t true u2 >>= fun _ ->
    Lwtc.failfmt "xxx" >>= fun () ->
    Lwt.return ()
  in
  let main_t () = lwt_wrap setup () test teardown in
  Lwt_main.run (main_t())

let suite = 
  "Bstore" >:::[
    "log" >:: t_log;
  ]
