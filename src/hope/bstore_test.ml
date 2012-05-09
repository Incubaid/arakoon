open OUnit
open Lwt
open Bstore

let t_log () =  
  let main_t() = 
    let fn = "t_log.bs" in
    BStore.init fn >>= fun () ->
    BStore.create fn >>= fun t ->
    let u = Core.SET("k","v") in
    BStore.log t true u >>= fun _ ->
    Lwtc.failfmt "xxx" >>= fun () ->
    Lwt.return ()
(*
  client = C.get_client()
  client.set('x','x')
  ass = arakoon.ArakoonProtocol.Assert('x','x')
  seq = arakoon.ArakoonProtocol.Sequence()
  seq.addUpdate(ass)
  client.sequence(seq)
*)
  in
  Lwt_main.run (main_t())

let suite = 
  "Bstore" >:::[
    "log" >:: t_log;
  ]
