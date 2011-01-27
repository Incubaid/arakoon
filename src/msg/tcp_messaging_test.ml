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
open Lwt_buffer

class player id (m:messaging) = 
  let make_msg kind i = create kind (string_of_int i) in
object(self)
  val mutable _lowest = None

  method _send target msg =
    Lwt_log.debug_f "%s sending message %s to %s" 
      id (string_of msg) target >>= fun () ->
    m # send_message msg id target 
      
  method serve ?(n=100) ?(lowest=0) opp = 
    let msg = make_msg "ping" n in 
    m # send_message msg "a" opp >>= 
    self # run lowest
  
  method multi_serve n targets =
    let msg = make_msg "ping" n in
    self # mcast_msg targets msg >>= fun () ->
    self # wait_for_response n targets
 
  method wait_for_response n  targets=
    m # recv_message id >>= fun (msg, source) ->
    let n' = int_of_string ( msg.payload ) in 
    Lwt_log.debug_f "n=%d n'=%d" n n' >>= fun () -> 
    if n' == n  
    then
      begin
      if n > 0 
      then
        self#multi_serve (n-1) targets 
      else
        Lwt.return()
      end
    else
      self#wait_for_response n targets

  method play_dead () =
    Lwt_unix.sleep 10000000.0 


  method mcast_msg targets msg =
    Lwt_list.iter_p (fun t -> self#_send t msg) targets
    
  method run (lowest:int) ()=

    let rec loop () =
      begin
	m # recv_message id  >>= fun (msg, source) ->
	let reply =
	  match msg.kind with
	    | "ping" ->
	      let i = int_of_string msg.payload in
	      let msg = make_msg "pong" i in
	      Some msg
	    | "pong" ->
	      let i = int_of_string msg.payload in
	      if i > lowest
	      then 
		let msg = make_msg "ping" (i-1) in
		Some msg
	      else 
		None 
	    | _ -> failwith "unknown message"
	in
	match reply with
	  | None -> Lwt.return ()
	  | Some msg -> 
	    self # _send source msg >>= fun () -> 
	    loop ()
      end
    in loop ()
end 
  
let make_transport address = 
  let tcp_transport = new tcp_messaging address in
  (tcp_transport :> messaging) 
    
let eventually_die ?(t=10.0) () = 
  Lwt_unix.sleep t >>= fun () -> OUnit.assert_failure "test takes too long:aborting"
  
let test_pingpong_1x1 () = 
  let port = 40010 in
  let address = ("127.0.0.1", port) in
  let transport = make_transport address in
  let mapping = [("a", address);] in
  let () = transport # register_receivers mapping in
  let player_a = new player "a" transport in
  let cvar = Lwt_condition.create () in
  let up_and_running () = Lwt_condition.broadcast cvar (); Lwt.return () in
  Lwt_main.run (Lwt.pick [ transport # run ~up_and_running (); 
			   begin 
			     Lwt_condition.wait cvar >>= fun () ->
			     Lwt_log.debug "going to serve" >>= fun () ->
			     player_a # serve "a"
			   end;
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
  Lwt_main.run (Lwt.pick [ transport_a # run ();
			   transport_b # run ();
			   player_a # serve "b";
			   player_b # run 0 ();
			   eventually_die ()
			 ])
    
let test_pingpong_multi_server () =
  let port_a = 40010
  and port_b = 40020  
  and port_c = 40030 in
  let address_a = ("127.0.0.1", port_a)
  and address_b = ("127.0.0.1", port_b)
  and address_c = ("127.0.0.1", port_c)
  in
  let transport_a = make_transport address_a in
  let transport_b = make_transport address_b in
  let transport_c = make_transport address_c in
  let mapping = [("a", address_a);
                 ("b", address_b);
                 ("c", address_c);] in
  let () = transport_a # register_receivers mapping in
  let () = transport_b # register_receivers mapping in
  let () = transport_c # register_receivers mapping in
  let player_a = new player "a" transport_a in
  let player_b = new player "b" transport_b in
  let player_c = new player "c" transport_c in
  let timeout = 60.0 in 
  Lwt_main.run (Lwt.pick [ transport_a # run ();
                           transport_b # run ();
                           transport_c # run ();
                           player_a # multi_serve 10000 ["b"; "c" ] ;
                           player_b # run 0 ();
                           player_c # play_dead ();
                           eventually_die ~t:timeout () 
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
  let main_t = 
    Lwt.pick [ 
      begin 
	Lwt.pick [ t_a # run ();
		   player_a # run 50 () >>= fun () -> Lwt_log.debug "a done" 
		 ]
	>>= fun () ->
	let t_a' = make_transport address_a in
	let () = t_a' # register_receivers mapping in
	let player_a' = new player "a" t_a' in
	Lwt.pick [
	  (Lwt_log.info "new network" >>= fun () -> 
	   t_a' # run ()); 
	  begin 
	    Lwt_log.debug "a' will be serving momentarily" >>= fun () ->
	    player_a' # serve ~n:200 "b" 
	  end
	]
      end;
      t_b # run ();
      player_a # serve "b";
      player_b # run 0 ();
      eventually_die () ]
    >>= fun () -> Lwt_log.info "main_t: after pick"
  in
  Lwt_main.run main_t

let suite = "tcp" >::: [
  "pingpong_1x1" >:: test_pingpong_1x1;
  "pingpong_2x2" >:: test_pingpong_2x2; 
  "pingpong_multi_server" >:: test_pingpong_multi_server;
  "pingpong_restart" >:: test_pingpong_restart;
]

