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



open OUnit
open Lwt

let section = Lwt_log.Section.main

let echo_protocol (ic,oc,_cid) =
  let size = 1024 in
  let buffer = Bytes.create size in
  let rec loop () =
    Lwt_io.read_into ic buffer 0 size >>= fun read ->
    Lwt_io.write_from_exactly oc buffer 0 read >>= loop
  in
  loop ()

let test_echo () =
  let sleep, notifier = wait () in
  let td_var = Lwt_mvar.create_empty () in
  let setup_callback () =
    Lwt.wakeup notifier () ;
    Lwt.return ()
  in
  let teardown_callback () = Lwt_mvar.put td_var () in
  let port = 6666 in
  let host = "127.0.0.1" in
  let scheme = Server.make_default_scheme () in
  let stop = ref false in
  let server = Server.make_server_thread
                 ~setup_callback host port echo_protocol
                 ~teardown_callback
                 ~scheme ~stop
                 ~tcp_keepalive:Tcp_keepalive.default_tcp_keepalive
  in
  let client () =
    Logger.debug_ "sleeping until server socket started" >>= fun () ->
    sleep >>= fun () ->
    Logger.debug_ "server is up & running" >>= fun () ->
    let conversation (ic,oc) =
      let words = ["e";"eo";"eoe";"eoebanibabaniwe";] in
      let test_one word =
        Lwt_io.write_line oc word >>= fun () ->
        Lwt_io.read_line ic >>= fun word' ->
        Logger.info_f_ "%S ? %S" word word'
      in Lwt_list.iter_s test_one words
    in
    let address = Unix.ADDR_INET(Unix.inet_addr_loopback, port) in
    Lwt_io.open_connection address >>= fun connection ->
    conversation connection >>= fun () ->
    Logger.info_ "end of conversation"
  in
  let main () =
    Lwt.pick [client (); server()] >>= fun () ->
    Lwt_mvar.take td_var  >>= fun () ->
    Logger.info_ "end_of_main"
  in
  Lwt_main.run (main())



let test_max_connections () =
  let sleep, notifier = wait () in
  let td_var = Lwt_mvar.create () in
  let setup_callback () =
    Lwt.wakeup notifier () ;
    Lwt.return ()
  in
  let echo_protocol2 (ic,oc,cid) =
    let rec loop () =
      Llio.input_string ic >>= fun s ->
      Logger.debug_f_ "%S read %S" cid s >>= fun () ->
      Llio.output_int32 oc 0l >>= fun () ->
      Llio.output_string oc s >>= fun () ->
      loop ()
    in
    loop()
  in

  let teardown_callback () = Lwt_mvar.put td_var () in
  let port = 6666 in
  let host = "127.0.0.1" in
  let scheme = Server.create_connection_allocation_scheme 2 in
  let stop = ref false in
  let server = Server.make_server_thread ~scheme
                 ~setup_callback ~teardown_callback
                 host port echo_protocol2 ~stop
                 ~tcp_keepalive:Tcp_keepalive.default_tcp_keepalive
  in
  let n_problems = ref 0 in
  let rc_x = ref None in
  let client i =
    Logger.debug_f_ "client %i: sleeping until server socket started" i >>= fun () ->
    sleep >>= fun () ->
    Logger.debug_f_ "client %i: server is up & running" i >>= fun () ->
    let conversation (ic,oc) =
      Logger.debug_f_ "start_of_conversation client %i" i >>= fun () ->
      let words = ["e";"eo";"eoe";"eoebanibabaniwe";] in
      let test_one word =
        Llio.output_string oc word >>= fun () ->
        Llio.input_int32 ic >>= fun rc ->
        begin
          match rc with
          | 0l -> ()
          | rc -> rc_x := Some rc
        end;
        Llio.input_string ic >>= fun word' ->
        Logger.info_f_ "client:%i %s ? (rc=%lx) %s" i word rc word'
      in
      Lwt.catch
        (fun () -> Lwt_list.iter_s test_one words >>= fun () -> Lwt_unix.sleep 0.1 )
        (function
          | Canceled as e -> Lwt.fail e
          | exn -> incr n_problems;Logger.info_f_ ~exn "client %i had problems" i)
    in
    let address = Unix.ADDR_INET(Unix.inet_addr_loopback, port) in
    (*    Lwt_io.with_connection address conversation  >>= fun () -> *)
    Lwt_io.open_connection address >>= fun conn ->
    conversation conn >>= fun () ->
    Logger.info_ "end of conversation."
  in
  let main_t =
    Lwt.pick [client 0;
              client 1;
              client 2;
              server();
              Lwt_unix.sleep 0.3
             ] >>= fun () ->
    let n_problems' = !n_problems in
    Lwt_mvar.take td_var >>= fun () ->
    Logger.debug_f_ "n_problems = %i" n_problems' >>= fun () ->
    OUnit.assert_equal n_problems' 1;
    OUnit.assert_equal !rc_x (Some 0xfel);
    Lwt.return ()
  in
  Lwt_main.run main_t


let suite = "server" >::: [
    "echo" >:: test_echo;
    "max_connections" >:: test_max_connections;
  ]
