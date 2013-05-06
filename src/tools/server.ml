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
open Lwt_log

let mv_waiter = Lwt_mvar.create_empty
let mv_callback = Lwt_mvar.put
let mv_wait = Lwt_mvar.take

let no_callback = Lwt.return

exception FOOBAR


let deny (ic,oc,cid) =
  Lwt_log.warning "max connections reached, denying this one" >>= fun () ->
  Llio.output_int oc 0xfe >>= fun () ->
  Llio.output_string oc "too many clients"
   

let session_thread (sid:string) cid protocol fd =
  Lwt.catch
    (fun () ->
      let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd
      and oc = Lwt_io.of_fd ~mode:Lwt_io.output fd
      in protocol (ic,oc,cid)
    )
    (function
      | FOOBAR as foobar-> 
          Lwt_log.fatal "propagating FOOBAR" >>= fun () ->
          Lwt.fail foobar
      | exn -> info_f ~exn "exiting session (%s) connection=%s" sid cid)
  >>= fun () -> 
  Lwt.catch 
    ( fun () -> Lwt_unix.close fd )
    ( fun exn -> Lwt_log.debug_f "Exception on closing of socket (connection=%s)" cid)

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

let make_server_thread 
    ?(name = "socket server")
    ?(setup_callback=no_callback) 
    ?(teardown_callback = no_callback)
    ~scheme
    host port protocol =
  let new_socket () = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let socket_address = Network.make_address host port in
  begin
    let listening_socket = new_socket () in
    Lwt_unix.setsockopt listening_socket Unix.SO_REUSEADDR true;
    Lwt_unix.bind listening_socket socket_address;
    Lwt_unix.listen listening_socket 1024;
    let maybe_take,release = scheme in
    let connection_counter = make_counter () in
    let rec server_loop () =
      Lwt.catch
	(fun () ->
	  Lwt_unix.accept listening_socket >>= fun (fd, cl_socket_address) ->
      let cid = name ^ "_" ^ Int64.to_string (connection_counter ()) in
          begin
            match maybe_take () with
              | None    -> Lwt.ignore_result (session_thread "--" cid deny fd)
              | Some id ->
	        Lwt.ignore_result
		  (
            Lwt_unix.fstat fd >>= fun fstat ->
            Lwt_log.info_f
              "%s:session=%i connection=%s socket_address=%s file_descriptor_inode=%i"
              name id cid (socket_address_to_string cl_socket_address) fstat.Lwt_unix.st_ino
            >>= fun () ->
            let sid = string_of_int id in
            session_thread sid cid protocol fd >>= fun () ->
            release();
            Lwt.return()
		  )
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
	    Lwt_log.warning_f 
	      "OUT OF FDS during accept (%s,%s) on port %i => sleeping %.1fs" 
	      s0 s1 port timeout
	    >>= fun () ->
	    Lwt_unix.sleep timeout
	  | e -> Lwt.fail e
	)
      >>= fun () ->
      server_loop ()
    in
    let r  = fun () ->
      Lwt.catch
	(fun ()  -> setup_callback () >>= fun () -> server_loop ())
	(fun exn -> info_f ~exn "shutting down server on port %i" port)
      >>= fun () ->
      Lwt_unix.close listening_socket >>= fun () ->
      teardown_callback()
    in r
  end
