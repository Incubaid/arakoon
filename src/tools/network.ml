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



open Lwt

let section = Logger.Section.main

let make_address host port =
  let ha = Unix.inet_addr_of_string host in
  Unix.ADDR_INET (ha, port)

let a2s = function
  | Unix.ADDR_INET (sa,p) -> Printf.sprintf "(%s,%i)" (Unix.string_of_inet_addr sa) p
  | Unix.ADDR_UNIX s      -> Printf.sprintf "ADDR_UNIX(%s)" s


let __open_connection ?(ssl_context : [> `Client ] Typed_ssl.t option)
                      ~tcp_keepalive
                      socket_address =
  let domain = Unix.domain_of_sockaddr socket_address in
  let socket = Lwt_unix.socket domain Unix.SOCK_STREAM 0  in
  Lwt.catch
    (fun () ->
       let () = Lwt_unix.setsockopt socket Lwt_unix.TCP_NODELAY true in
       let () = Tcp_keepalive.apply socket tcp_keepalive in
       Lwt_unix.connect socket socket_address >>= fun () ->
       let a2 = Lwt_unix.getsockname socket in
       let peer = Lwt_unix.getpeername socket in
       begin
         if (a2 = peer)
         then Llio.lwt_failfmt "a socket should not connect to itself"
         else Lwt.return ()
       end
       >>= fun () ->
       let fdi = Unix_fd.lwt_unix_fd_to_fd socket in
       let peer_s = a2s peer in
       Logger.info_f_ "__open_connection SUCCEEDED (fd=%i) %s %s" fdi
         (a2s a2) peer_s
       >>= fun () ->
       match ssl_context with
         | None ->
             let ic = Lwt_io.of_fd ~mode:Lwt_io.input  socket
             and oc = Lwt_io.of_fd ~mode:Lwt_io.output socket in
             Lwt.return (socket, ic, oc)
         | Some ctx ->
             Typed_ssl.Lwt.ssl_connect socket ctx >>= fun (s, lwt_s) ->
             let cert = Ssl.get_certificate s in
             Logger.info_f_
               "__open_connection: SSL connection to %s succeeded, issuer=%s, subject=%s"
               peer_s (Ssl.get_issuer cert) (Ssl.get_subject cert) >>= fun () ->
             let cipher = Ssl.get_cipher s in
             Logger.debug_f_
               "__open_connection: SSL connection to %s using %s"
               peer_s (Ssl.get_cipher_description cipher) >>= fun () ->
             let ic = Lwt_ssl.in_channel_of_descr lwt_s
             and oc = Lwt_ssl.out_channel_of_descr lwt_s in
             Lwt.return (socket, ic, oc)
       )
    (fun exn ->
      Logger.info_f_ "__open_connection to %s failed: %S"
                     (a2s socket_address)
                     (Printexc.to_string exn)
       >>= fun () ->
       Lwt_unix.close socket >>= fun () ->
       Lwt.fail exn)
