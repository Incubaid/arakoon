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

open OUnit
open Lwt
open Lwt_log

let test_echo () = 
  let echo_protocol (ic,oc) = 
    let size = 1024 in
    let buffer = String.create size in
    let rec loop () = 
      Lwt_io.read_into ic buffer 0 size >>= fun read ->
      Lwt_io.write_from_exactly oc buffer 0 read >>= loop
    in
    loop ()
  in
  let sleep, notifier = wait () in
  let setup_callback () = 
    Lwt.wakeup notifier () ; 
    Lwt.return () 
  in
  let port = 6666 in
  let server = Server.make_server_thread ~setup_callback port echo_protocol in
  let client () = 
    debug "sleeping until server socket started" >>= fun () -> 
    sleep >>= fun () ->
    debug "server is up & running" >>= fun () ->
    let conversation (ic,oc) = 
      let words = ["e";"eo";"eoe";"eoebanibabaniwe";] in
      let test_one word = 
	Lwt_io.write_line oc word >>= fun () ->
	Lwt_io.read_line ic >>= fun word' ->
        Lwt_io.printlf "%s ? %s" word word'
      in Lwt_list.iter_s test_one words 
    in
    let address = Unix.ADDR_INET(Unix.inet_addr_loopback, port) in
    Lwt_io.open_connection address >>= fun connection ->
    conversation connection >>= fun () ->
    info "end of conversation" 
  in 
  Lwt_main.run (Lwt.pick [client ();
			  server()]);;

let suite = "server" >::: [
  "echo" >:: test_echo;
]
  
