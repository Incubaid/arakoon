open OUnit
open Lwt
open Bstore
open Core

let t_log () =  
  let main_t() = 
    let fn = "t_log.bs" in
    BStore.init fn >>= fun () ->
    BStore.create fn >>= fun t ->
    let u = SET("x","x") in
    BStore.log t true u >>= fun _ ->
    let u2 = SEQUENCE [ASSERT("x",Some "x")] in
    BStore.log t true u2 >>= fun _ ->
    Lwtc.failfmt "xxx" >>= fun () ->
    Lwt.return ()
  in
  Lwt_main.run (main_t())

let suite = 
  "Bstore" >:::[
    "log" >:: t_log;
  ]
