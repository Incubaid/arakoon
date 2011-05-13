open Lwt
open OUnit
open Node_cfg
open Master_type
open Test_backend

let setup () = Lwt.return ()

let teardown () = Lwt.return ()


let _cluster = "baby1"
let _port = 5000 

let __wrap__ conversation = 
  let sleep, notifier = Lwt.wait () in
  let tb = new test_backend _cluster in
  let backend = (tb :> Backend.backend) in
  let setup_callback ()  = 
    Lwt_log.info "callback" >>= fun () ->
    Lwt.wakeup notifier ();
    Lwt.return ()
  in
  let server = 
    Server.make_server_thread ~setup_callback "127.0.0.1" _port 
      (Client_protocol.protocol backend) in
  let client_t () =
    sleep >>= fun () ->
    let address = Unix.ADDR_INET (Unix.inet_addr_loopback, _port) in
    Lwt_io.open_connection address >>= function (ic,oc) ->
      conversation (ic,oc) >>= fun () ->
      Lwt_log.debug "end_of_senario" >>= fun () ->
      Lwt_io.close ic >>= fun () ->
      Lwt_io.close oc >>= fun () ->
      Lwt.return ()
  in
  Lwt.pick [client_t ();server ()] >>= fun () ->
  Lwt.return ()

let set_range () = 
  let conversation (ic,oc) = 
    Lwt_log.debug "starting set_range ..." >>= fun () ->
    Lwt_unix.sleep 4.0 
  in
  __wrap__ conversation

let suite = 
  let w f = Extra.lwt_bracket setup f teardown in
  "nursery" >:::
    ["set_range" >:: w set_range;
    ]

