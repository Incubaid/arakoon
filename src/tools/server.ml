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


let session_thread protocol fd = 
  info "starting session " >>= fun () ->
  let close () = Lwt_unix.close fd; Lwt.return () in
  Lwt.catch
    (fun () ->
      let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd
      and oc = Lwt_io.of_fd ~mode:Lwt_io.output fd
      in protocol (ic,oc) 
    )
    (fun exn -> info ~exn "exiting session")
  >>= close 
    
  


let make_server_thread ?(setup_callback=no_callback) port protocol =
  let new_socket () = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let local_addr num = Unix.ADDR_INET (Unix.inet_addr_any, num) in
  begin
    let listening_socket = new_socket () in
    Lwt_unix.setsockopt listening_socket Unix.SO_REUSEADDR true;
    Lwt_unix.bind listening_socket (local_addr port);
    Lwt_unix.listen listening_socket 1024;
    let rec server_loop () =
      info "await connection" >>= fun () ->
      Lwt_unix.accept listening_socket >>= fun (fd, _) ->
      (Lwt.ignore_result (session_thread protocol fd));
      server_loop ()
    in
    let r  = fun () ->
      Lwt.catch
	(fun () -> setup_callback () >>= fun () -> server_loop ())
	(fun exn -> info ~exn "shutting down")
	     >>= fun () ->
      Lwt_unix.close listening_socket;
      Lwt.return ()
    in r
  end
