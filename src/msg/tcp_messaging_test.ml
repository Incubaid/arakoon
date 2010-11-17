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

open Messaging
open Message.Message
open Tcp_messaging
open Lwt
open OUnit
open Log_extra

class player id (m:messaging) = 
  let make_msg kind i = create kind (string_of_int i) in
object(self)
  val mutable _lowest = None

  method _send target msg =
    Lwt_log.debug_f "%s sending message %s to %s" 
      id (string_of msg) target >>= fun () ->
    m # send_message msg id target 
      
  method serve ?(n=100) opp = 
    let msg = make_msg "ping" n in 
    m # send_message msg "a" opp >>= 
    self # run 
      
  method get_lowest () = _lowest
  method private _maybe_update i =
    match _lowest with
      | None -> _lowest <- Some i
      | Some j -> if i < j then _lowest <- Some i

    
  method run () =
    m # recv_message id  >>= fun (msg, source) ->
    match msg.kind with
      | "ping" ->
	let i = int_of_string msg.payload in
	let () = self # _maybe_update i in
	let msg = make_msg "pong" i in
        (self #_send source msg >>= self # run )
      | "pong" ->
	let i = int_of_string msg.payload in
	let () = self # _maybe_update i in
	if i > 0 then 
	  let msg = make_msg "ping" (i-1) in
	  self # _send source msg
	   >>= self # run 
	else Lwt.return () 
      | _ -> Lwt.fail (Failure "unknown message")
end 
  
let make_transport address = 
  let tcp_transport = new tcp_messaging address in
  (tcp_transport :> messaging) 
    
let eventually_die () = 
  Lwt_unix.sleep 10.0 >>= fun () -> OUnit.assert_failure "test takes too long:aborting"
  
let test_pingpong_1x1 () = 
  let port = 40010 in
  let address = ("127.0.0.1", port) in
  let transport = make_transport address in
  let mapping = [("a", address);] in
  let () = transport # register_receivers mapping in
  let player_a = new player "a" transport in
  let stop () = false in
  Lwt_main.run (Lwt.pick [ transport # run ~stop (); (* needs callback *)
			   player_a # serve "a";
			   eventually_die ()
			 ])
    
let test_pingpong_2x2 () = 
  let port_a = 40010 
  and port_b = 40020  in
  let address_a = ("127.0.0.1", port_a)
  and address_b = ("127.0.0.1", port_b) 
  in
  let transport_a = make_transport address_a in
  let transport_b = make_transport address_b in
  let mapping = [("a", address_a);
		 ("b", address_b);] in
  let () = transport_a # register_receivers mapping in
  let () = transport_b # register_receivers mapping in
  let player_a = new player "a" transport_a in
  let player_b = new player "b" transport_b in
  let never () = false in
  Lwt_main.run (Lwt.pick [ transport_a # run ~stop:never ();
			   transport_b # run ~stop:never ();
			   player_a # serve "b";
			   player_b # run ();
			   eventually_die ()
			 ])
    

let test_pingpong_restart () = 
  let port_a = 40010
  and port_b = 40020 in
  let address_a = ("127.0.0.1", port_a)
  and address_b = ("127.0.0.1", port_b) 
  in
  let t_a = make_transport address_a in
  let t_b = make_transport address_b in
  let mapping = [("a", address_a);
		 ("b", address_b);] in
  let () = t_a # register_receivers mapping in
  let () = t_b # register_receivers mapping in
  let player_a = new player "a" t_a in
  let player_b = new player "b" t_b in
  let a_stop () = 
    let lp = player_a # get_lowest () in
    match lp with
      | Some i when i = 50 -> true
      | _ -> false
  in
  let never () = false in
  let restart_a () = 
    t_a # run ~stop:a_stop () >>= fun () ->
    Lwt_log.info "should restart networking" >>= fun () ->
    let t_a' = make_transport address_a in
    let () = t_a' # register_receivers mapping in
    let player_a' = new player "a" t_a' in
    Lwt.pick [(Lwt_log.info "new network" >>= fun () -> t_a' # run ~stop:never ()); 
	      player_a' # serve ~n:200 "b"]
    >>= fun () ->
    Lwt_log.info "restart_a: after pick"
  in
  let main_t = 
    Lwt.pick [ restart_a () ;
	       t_b # run ~stop:never ();
	       player_a # serve "b";
	       player_b # run () ;
	       eventually_die () ]
      >>= fun () -> Lwt_log.info "main_t: after pick"
  in
  Lwt_main.run main_t

let suite = "tcp" >::: [
  "pingpong_1x1" >:: test_pingpong_1x1;
  "pingpong_2x2" >:: test_pingpong_2x2; 
  "pingpong_restart" >:: test_pingpong_restart;
]

