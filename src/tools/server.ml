(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
*)

open Lwt

let section = Logger.Section.main

let mv_waiter = Lwt_mvar.create_empty
let mv_callback = Lwt_mvar.put
let mv_wait = Lwt_mvar.take

let no_callback = Lwt.return

exception FOOBAR

type socket = Plain of Lwt_unix.file_descr
            | TLS of Lwt_ssl.socket

let close = function
  | Plain fd -> Lwt_unix.close fd
  | TLS fd -> Lwt_ssl.close fd

let deny (ic,oc,cid) =
  Logger.warning_ "max connections reached, denying this one" >>= fun () ->
  Llio.output_int oc 0xfe >>= fun () ->
  Llio.output_string oc "too many clients"


let session_thread (sid:string) cid protocol fd =
  Lwt.catch
    (fun () ->
       let (ic, oc) = match fd with
         | Plain fd' ->
             let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd'
             and oc = Lwt_io.of_fd ~mode:Lwt_io.output fd' in
             (ic, oc)
         | TLS fd' ->
             let ic = Lwt_ssl.in_channel_of_descr fd'
             and oc = Lwt_ssl.out_channel_of_descr fd' in
             (ic, oc)
       in protocol (ic,oc,cid)
    )
    (function
      | FOOBAR as foobar->
        Logger.fatal_ "propagating FOOBAR" >>= fun () ->
        Lwt.fail foobar
      | Canceled ->
        Lwt.fail Canceled
      | exn ->
        Logger.info_f_ ~exn "exiting session (%s) connection=%s" sid cid)
  >>= fun () ->
  Lwt.catch
    ( fun () -> close fd )
    ( function
      | Canceled -> Lwt.fail Canceled
      | exn -> Logger.info_f_ ~exn "Exception on closing of socket (connection=%s)" cid)

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
       (fun () -> close sock)
       (fun exn ->
        let level = match exn with
          | Unix.Unix_error(Unix.EBADF, _, _) -> Logger.Debug
          | _ -> Logger.Info in
        Logger.log_ ~exn section level (fun () -> Printf.sprintf "Exception while closing client fd %s" cid))
    )
  
let make_server_thread
      ?(name = "socket server")
      ?(setup_callback=no_callback)
      ?(teardown_callback = no_callback)
      ?(ssl_context : [> `Server ] Typed_ssl.t option)
      ~scheme
      host port protocol =
  
  let socket_address = Network.make_address host port in
  begin
    let listening_socket = new_socket socket_address in
    Lwt_unix.setsockopt listening_socket Unix.SO_REUSEADDR true;
    Lwt_unix.bind listening_socket socket_address;
    Lwt_unix.listen listening_socket 1024;
    let () = match ssl_context with
      | None -> ()
      | Some ctx ->
          let _ = Typed_ssl.embed_socket (Lwt_unix.unix_file_descr listening_socket) in
          ()
    in
    let maybe_take,release = scheme in
    let connection_counter = make_counter () in
    let client_threads = Hashtbl.create 10 in
    let _condition = Lwt_condition.create () in
    let going_down = ref false in
    let inner_take () =
      if !going_down
      then None
      else maybe_take () 
    in
    
    let possible_denial cid plain_fd cl_socket_address =
      make_socket plain_fd ssl_context >>= fun sock ->
      match inner_take () with
      | None ->  
         begin
           _socket_closer cid sock (fun () -> session_thread "--" cid deny sock)
           >>= fun () ->
           Lwt.return None
         end
      | Some id ->  
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
    let rec server_loop () =
      Lwt.catch
        (fun () ->
           Lwt_unix.accept listening_socket >>= fun (plain_fd, cl_socket_address) ->
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
                      Logger.info_f_ ~exn "Exception in client thread %s" cid >>= fun () ->
                      finalize ()))
             | None -> ()
           end;
           server_loop ()           
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
            Lwt_unix.sleep timeout >>= fun () ->
            server_loop ()
          | Ssl.Accept_error _ as exn ->
              Logger.warning_f_ ~exn
                "Ssl.Accept_error in server_loop: %s"
                (Ssl.get_error_string ()) >>=
              server_loop
          | e -> Lwt.fail e
        )
    in
    let r  = fun () ->
      Lwt.finalize
        (fun ()  ->
           setup_callback () >>= fun () ->
           server_loop ())
        (fun () ->
           going_down := true;
           Lwt.catch
             (fun () ->
                Logger.debug_ "closing listening socket" >>= fun () ->
                Lwt_unix.close listening_socket >>= fun () ->
                Logger.debug_ "closed listening socket")
             (fun exn ->
                Logger.info_f_ ~exn "exception while closing listening socket") >>= fun () ->

           let cancel _ t =
             try
               Lwt.cancel t
             with exn -> () in
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
