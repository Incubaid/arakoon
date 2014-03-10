(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

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
open OUnit

let test_leak () =
  let listening_socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let () = Lwt_unix.setsockopt listening_socket Unix.SO_REUSEADDR true in
  let () = Lwt_unix.bind listening_socket (Unix.ADDR_INET (Unix.inet_addr_any,
                                                           6688)) in
  let () = Lwt_unix.listen listening_socket 10 in
  Lwt.join
    [
      begin
        Lwt_unix.accept listening_socket >>= fun (fd,_addr) ->
        Lwt_unix.wait_read fd >>= fun () ->
        Lwt_unix.close fd >>= fun () ->
        Lwt_unix.close listening_socket >>= fun () ->
        Lwt.return ()
      end;
      begin
        Lwt_io.open_connection (Unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1",
                                                6688)) >>= fun (ic,oc) ->
        Lwt.finalize
          (fun () ->
             Lwt.catch
               (fun () ->
                  Lwt_io.write_char oc 'c' >>= fun () ->
                  Lwt_unix.sleep 0.1 >>= fun () ->
                  Lwt_io.write_char oc 'd' >>= fun () ->
                  Lwt.return ())
               (function
                 | End_of_file -> Lwt.return ()
                 | e -> Lwt.fail e
               )
          )
          (fun () ->
             Lwt_io.close ic >>= fun () ->
             Lwt_io.close oc
             (* supposedly connection completely closed,
                actually shutdown will fail *)
          )
      end
    ]

let wrap t =
  Extra.lwt_bracket
    (fun () -> Lwt.return ())
    t
    (fun () -> Lwt.return ())
let suite = "server_socket" >:::[
    "leak" >:: wrap test_leak
  ]
