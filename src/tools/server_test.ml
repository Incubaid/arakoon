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

let echo_protocol (ic,oc) = 
  let size = 1024 in
  let buffer = String.create size in
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
  let server = Server.make_server_thread 
    ~setup_callback host port echo_protocol 
    ~teardown_callback
    ~scheme
  in
  let client () = 
    debug "sleeping until server socket started" >>= fun () -> 
    sleep >>= fun () ->
    debug "server is up & running" >>= fun () ->
    let conversation (ic,oc) = 
      let words = ["e";"eo";"eoe";"eoebanibabaniwe";] in
      let test_one word = 
	    Lwt_io.write_line oc word >>= fun () ->
	    Lwt_io.read_line ic >>= fun word' ->
        Lwt_log.info_f "%s ? %s" word word'
      in Lwt_list.iter_s test_one words 
    in
    let address = Unix.ADDR_INET(Unix.inet_addr_loopback, port) in
    Lwt_io.open_connection address >>= fun connection ->
    conversation connection >>= fun () ->
    info "end of conversation" 
  in 
  let main () = 
    Lwt.pick [client (); server()] >>= fun () -> 
    Lwt_mvar.take td_var  >>= fun () ->
    Lwt_log.info "end_of_main" 
  in
  Lwt_main.run (main())



let test_max_connections () = 
  let sleep, notifier = wait () in
  let td_var = Lwt_mvar.create () in
  let setup_callback () = 
    Lwt.wakeup notifier () ; 
    Lwt.return () 
  in
 
  let teardown_callback () = Lwt_mvar.put td_var () in
  let port = 6666 in
  let host = "127.0.0.1" in
  let scheme = Server.create_connection_allocation_scheme 2 in
  let server = Server.make_server_thread ~scheme
    ~setup_callback ~teardown_callback
    host port echo_protocol 
  in
  let n_problems = ref 0 in
  let client i = 
    debug_f "client %i sleeping until server socket started" i >>= fun () -> 
    sleep >>= fun () ->
    debug "server is up & running" >>= fun () ->
    let conversation (ic,oc) = 
      debug_f "start_of_conversation client %i" i >>= fun () ->
      let words = ["e";"eo";"eoe";"eoebanibabaniwe";] in
      let test_one word = 
	    Lwt_io.write_line oc word >>= fun () ->
	    Lwt_io.read_line ic >>= fun word' ->
        Lwt_log.info_f "%s ? %s" word word'
      in 
      Lwt.catch
	    (fun () -> Lwt_list.iter_s test_one words >>= fun () -> Lwt_unix.sleep 0.1 )
	    (function 
	      | Canceled as e -> Lwt.fail e
	      | exn -> incr n_problems;Lwt_log.info_f ~exn "client %i had problems" i)
    in
    let address = Unix.ADDR_INET(Unix.inet_addr_loopback, port) in
    (*    Lwt_io.with_connection address conversation  >>= fun () -> *)
    Lwt_io.open_connection address >>= fun conn ->
    conversation conn >>= fun () ->
    info "end of conversation." 
  in 
  let main_t = 
    Lwt.pick [client 0;
	      client 1;
	      client 2;
	      server();
          Lwt_unix.sleep 0.3
             ] 
    >>= fun () -> Lwt_mvar.take td_var >>= fun () ->
    Lwt_log.debug_f "n_problems = %i" !n_problems >>= fun () ->
    OUnit.assert_equal !n_problems 1;
    Lwt.return () 
  in
  Lwt_main.run main_t


let suite = "server" >::: [
  "echo" >:: test_echo;
  "max_connections" >:: test_max_connections;
]
  
