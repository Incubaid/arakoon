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

let section =
  let s = Logger.Section.make "server" in
  let () = Logger.Section.set_level s Logger.Debug in
  s

let no_callback = Lwt.return

exception FOOBAR

type socket = Plain of Lwt_unix.file_descr
            | TLS of Lwt_ssl.socket

let close = function
  | Plain fd -> Lwt_unix.close fd
  | TLS fd -> Lwt_ssl.close fd

let deny_max (_ic,oc,_cid) =
  Logger.warning_ "max connections reached, denying this one" >>= fun () ->
  Llio.output_int32 oc (Arakoon_exc.int32_of_rc Arakoon_exc.E_MAX_CONNECTIONS) >>= fun () ->
  Llio.output_string oc "too many clients"

let deny_closing (_ic,oc,_cid) =
  Logger.warning_ "closing socket, denying this one" >>= fun () ->
  Llio.output_int32 oc (Arakoon_exc.int32_of_rc Arakoon_exc.E_GOING_DOWN) >>= fun () ->
  Llio.output_string oc "closing socket"

let session_thread (sid:string) cid protocol fd =
  Lwt.catch
    (fun () ->
       let (ic, oc) = match fd with
         | Plain fd' ->
             let ic = Lwt_io.of_fd ~close:Lwt.return ~mode:Lwt_io.input fd'
             and oc = Lwt_io.of_fd ~close:Lwt.return ~mode:Lwt_io.output fd' in
             (ic, oc)
         | TLS fd' ->
             let ic = Lwt_ssl.in_channel_of_descr fd'
             and oc = Lwt_ssl.out_channel_of_descr fd' in
             (ic, oc)
       in
       Lwt.finalize
         (fun () -> protocol (ic,oc,cid))
         (fun () ->
          Lwt_io.close oc >>= fun () ->
          Lwt_io.close ic)
    )
    (function
      | FOOBAR as foobar->
        Logger.fatal_ "propagating FOOBAR" >>= fun () ->
        Lwt.fail foobar
      | Canceled ->
        Lwt.fail Canceled
      | exn ->
         Logger.info_f_
           "exiting session (%s) connection=%s: %S"
           sid cid (Printexc.to_string exn))

let create_connection_allocation_scheme max =
  let counter = ref 0 in
  let maybe_take () =
    let c = !counter in
    if c = max
    then None
    else let () = incr counter in Some c
  and release () = decr counter
  in maybe_take, release

let make_default_scheme () = create_connection_allocation_scheme 10

let make_counter () =
  let c = ref 0L in
  let next () =
    c := Int64.succ !c;
    !c in
  next

let socket_address_to_string = function
  | Unix.ADDR_UNIX s -> Printf.sprintf "ADDR_UNIX %s" s
  | Unix.ADDR_INET (inet_addr, port) -> Printf.sprintf "ADDR_INET %s,%i" (Unix.string_of_inet_addr inet_addr) port


let make_socket plain_fd = function
  | None -> Lwt.return (Plain plain_fd)
  | Some ctx ->
     Lwt.catch
       (fun () ->
        Typed_ssl.Lwt.ssl_accept plain_fd ctx >>= fun (s, lwt_s) ->
        let get_certificate s =
          try
            let c = Ssl.get_certificate s in
            Some c
          with Ssl.Certificate_error -> None
        in
        begin match get_certificate s with
              | None ->
                 Logger.info_f_
                   "server_loop: Received SSL connection without peer certificate"
              | Some cert ->
                 Logger.info_f_
                   "server_loop: Received SSL connection, subject=%s"
                   (Ssl.get_subject cert)
        end >>= fun () ->
        let cipher = Ssl.get_cipher s in
        Logger.debug_f_
          "server_loop: SSL connection using %s"
          (Ssl.get_cipher_description cipher) >>= fun () ->
        Lwt.return (TLS lwt_s))
       (fun exn ->
        Lwt_unix.close plain_fd >>= fun () ->
        Lwt.fail exn)

let new_socket sa =
    let domain = Unix.domain_of_sockaddr sa in
    Lwt_unix.socket domain Unix.SOCK_STREAM 0

let _socket_closer cid sock f =
  Lwt.finalize
    f
    (fun () ->
     Lwt.catch
       (fun () ->
        Logger.info_f_ "%s: closing" cid >>= fun () ->
        close sock)
       (function
         | Canceled ->
            Lwt.fail Canceled
         | exn ->
            let level = match exn with
              | Unix.Unix_error(Unix.EBADF, _, _) -> Logger.Debug
              | _ -> Logger.Info in
            Logger.log_
              section level
              (fun () -> Printf.sprintf "Exception while closing client fd %s: %S"
                                        cid (Printexc.to_string exn))
       )
    )

let make_server_thread
      ?(name = "socket server")
      ?(setup_callback=no_callback)
      ?(teardown_callback = no_callback)
      ?(ssl_context : [> `Server ] Typed_ssl.t option)
      ~tcp_keepalive
      ~scheme ~stop
      host port protocol =

  let socket_address = Network.make_address host port in
  let maybe_take,release = scheme in
  let connection_counter = make_counter () in
  let client_threads = Hashtbl.create 10 in
  let _condition = Lwt_condition.create () in
  let bind () =
      let listening_socket = new_socket socket_address in
      Lwt_unix.setsockopt listening_socket Unix.SO_REUSEADDR true;
      Lwt_unix.bind listening_socket socket_address >>= fun () ->
      Lwt_unix.listen listening_socket 1024;
      let () =
        match ssl_context with
        | None -> ()
        | Some _ctx ->
           let _ = Typed_ssl.embed_socket
                     (Lwt_unix.unix_file_descr listening_socket)
           in
           ()
      in
      Lwt.return listening_socket
  in
  begin
    let possible_denial cid plain_fd cl_socket_address =
      make_socket plain_fd ssl_context >>= fun sock ->
      match !stop, maybe_take () with
      | true, _ ->
         begin
           _socket_closer cid sock (fun () -> session_thread "--" cid deny_closing sock)
           >>= fun () ->
           Lwt.fail Canceled
         end
      | false, None ->
         begin
           _socket_closer cid sock (fun () -> session_thread "--" cid deny_max sock)
           >>= fun () ->
           Lwt.return None
         end
      | false, Some id ->
         begin
           let t =
             _socket_closer
               cid sock
               (fun () ->
                (* Lwt_unix.fstat plain_fd >>= fun fstat -> *)
                Logger.info_f_
                  "%s:session=%i connection=%s socket_address=%s" (* " file_descriptor_inode=%i" *)
                  name id cid (socket_address_to_string cl_socket_address) (* fstat.Lwt_unix.st_ino *)
                >>= fun () ->
                let sid = string_of_int id in
                session_thread sid cid protocol sock >>= fun () ->
                release ();
                Lwt.return ())
           in
           Lwt.return (Some t)
         end
    in
    let rec server_loop listening_socket =
      let serve () =
        Lwt.catch
          (fun () ->
            Lwt_unix.accept listening_socket >>= fun (plain_fd, cl_socket_address) ->
            Lwt_unix.setsockopt plain_fd Lwt_unix.TCP_NODELAY true;
            Tcp_keepalive.apply plain_fd tcp_keepalive;
            let cid = name ^ "_" ^ Int64.to_string (connection_counter ()) in
            let finalize () =
              Hashtbl.remove client_threads cid;
              Lwt_condition.signal _condition ();
              Lwt.return ()
            in
            possible_denial cid plain_fd cl_socket_address >>= fun mt ->
            begin
              match mt with
              | Some t ->
                 Lwt.ignore_result
                   (Lwt.catch
                      (fun () ->
                        Hashtbl.add client_threads cid t;
                        t >>= fun () ->
                        finalize ()
                      )
                      (fun exn ->
                        Logger.info_f_
                          "Exception in client thread %s: %S"
                          cid
                          (Printexc.to_string exn)
                        >>= fun () ->
                        finalize ()))
              | None -> ()
            end;
            Lwt.return ()
          )
          (function
            | Unix.Unix_error (Unix.EMFILE,s0,s1) ->
               let timeout = 4.0 in
               (* if we don't sleep, this will go into a spinning loop of
               failfasts;
               we want to block until an fd is available,
               but alas, I found no such API.
                *)
               Logger.warning_f_
                 "OUT OF FDS during accept (%s,%s) on port %i => sleeping %.1fs"
                 s0 s1 port timeout >>= fun () ->
               Lwt_unix.sleep timeout
            | Ssl.Accept_error _ ->
               Logger.warning_f_ "Ssl.Accept_error in server_loop: %s"
                                 (Ssl.get_error_string ())

            | e -> Lwt.fail e
           )
      in
      serve () >>= fun () ->
      server_loop listening_socket
    in
    let r  = fun () ->
      bind () >>= fun listening_socket ->

      Lwt.finalize
        (fun ()  ->
           setup_callback () >>= fun () ->
           server_loop listening_socket)
        (fun () ->
           Lwt.catch
             (fun () ->
                Logger.debug_ "closing listening socket" >>= fun () ->
                Lwt_unix.close listening_socket >>= fun () ->
                Logger.debug_ "closed listening socket")
             (fun exn ->
               Logger.info_f_ "exception while closing listening socket: %S"
                              (Printexc.to_string exn)
             )
           >>= fun () ->

           let cancel _ t =
             try
               Lwt.cancel t
             with _exn -> () in
           Hashtbl.iter cancel client_threads;

           let rec wait () =
             Logger.info_f_ "waiting for %i client_threads" (Hashtbl.length client_threads) >>= fun () ->
             if Hashtbl.length client_threads > 0
             then
               begin
                 Hashtbl.iter cancel client_threads;
                 Lwt_condition.wait _condition >>= fun () ->
                 wait ()
               end
             else
               Lwt.return () in
           wait () >>= fun () ->

           Logger.info_f_ "shutting down server on port %i" port >>= fun () ->
           teardown_callback())
    in r
  end
