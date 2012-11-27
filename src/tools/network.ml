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


let make_address host port =
  let ha = Unix.inet_addr_of_string host in
  Unix.ADDR_INET (ha, port) 

let a2s = function
  | Unix.ADDR_INET (sa,p) -> Printf.sprintf "(%s,%i)" (Unix.string_of_inet_addr sa) p
  | Unix.ADDR_UNIX s      -> Printf.sprintf "ADDR_UNIX(%s)" s

let __open_connection socket_address =
  (* Lwt_io.open_connection socket_address *)
  let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0  in
  let () = Lwt_unix.setsockopt socket Lwt_unix.TCP_NODELAY true in
  Lwt.catch
    (fun () ->
      Lwt_unix.connect socket socket_address >>= fun () ->
      let a2 = Lwt_unix.getsockname socket in
      let peer = Lwt_unix.getpeername socket in
      begin
	    if (a2 = peer) 
        then Llio.lwt_failfmt "a socket should not connect to itself"
	    else Lwt.return ()
      end 
      >>= fun () ->
      let fd_field = Obj.field (Obj.repr socket) 0 in
      let (fdi:int) = Obj.magic (fd_field) in
      Lwt_log.info_f "__open_connection SUCCEEDED (fd=%i) %s %s" fdi
	    (a2s a2) (a2s peer)
      >>= fun () ->
      let oc = Lwt_io.of_fd ~mode:Lwt_io.output socket in
      let ic = Lwt_io.of_fd ~mode:Lwt_io.input  socket in
      Lwt.return (ic,oc))
    (fun exn -> 
      Lwt_log.info_f ~exn "__open_connection to %s failed" (a2s socket_address)
      >>= fun () ->
      Lwt_unix.close socket >>= fun () ->
      Lwt.fail exn)
