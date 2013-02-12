open OUnit
open Lwt
open Bstore
open Core

let lwt_wrap setup init f teardown =
  setup init >>= fun r ->
  Lwt.catch
    (fun () -> f r >>= fun () -> teardown true r)
    (fun e -> 
      teardown false r >>= fun () -> 
      Lwt.fail e)
  

let setup () =
  let fn = Filename.temp_file "t_log_" ".bs" in
  Lwt_io.printlf "fn=%s\n" fn >>= fun () ->
  BStore.init fn >>= fun () ->
  BStore.create fn false >>= fun t ->
  Lwt.return (fn,t)

let teardown ok (fn,t) =
  BStore.close t >>= fun () ->
  if ok 
  then Lwt_unix.unlink fn
  else Lwt.return ()

  
let t_log () =  
  let test (fn,t) = 
    let fn = Filename.temp_file"t_log_" ".bs" in
    BStore.init fn >>= fun () ->
    BStore.create fn false >>= fun t ->
    let u = SET("x","x") in
    BStore.log t true u >>= fun _ ->
    let u2 = SEQUENCE [ASSERT("x",Some "x")] in
    let u3 = SEQUENCE [ASSERT_EXISTS("x")] in
    BStore.log t true u2 >>= fun _ ->
    BStore.log t true u3 >>= fun _ ->
    Lwt.return ()
  in
  let main_t () = lwt_wrap setup () test teardown in
  Lwt_main.run (main_t())


let t_test_and_set () =
  let test(fn,t) = 
    let fn = Filename.temp_file "t_test_and_set_" ".bs" in
    BStore.init fn >>= fun () ->
    BStore.create fn false >>= fun t ->
    let key = "x" in
    let u = TEST_AND_SET(key ,None, Some"X") in
    BStore.log t true u >>= fun _ ->
    let u2 = TEST_AND_SET (key, Some "Y", Some "Z") in
    BStore.log t true u2 >>= fun _ ->
    BStore.commit t (Core.ITick.from_int64 2L) >>= fun _ ->
    BStore.get t key >>= fun vo ->
    Lwtc.log "got %s" (Log_extra.string_option_to_string vo) >>= fun () ->
    OUnit.assert_equal ~printer:Log_extra.string_option_to_string vo (Some "X");
    Lwt.return ()
  in
  let main_t () = lwt_wrap setup () test teardown in
  Lwt_main.run (main_t())

let suite = 
  "Bstore" >:::[
    "log" >:: t_log;
    "test_and_set" >:: t_test_and_set; 
  ]
