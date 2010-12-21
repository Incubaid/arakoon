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

open Message
open Messaging
open Lwt
open Log_extra
open Lwt_buffer
open Lwtq

let a2s = function
  | Unix.ADDR_INET (sa,p) -> Printf.sprintf "(%s,%i)"
    (Unix.string_of_inet_addr sa) p
  | Unix.ADDR_UNIX s -> Printf.sprintf "ADDR_UNIX(%s)" s

let __open_connection socket_address =
  (* Lwt_io.open_connection socket_address *)
  let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0  in
  Lwt.catch
    (fun () ->
      Lwt_unix.connect socket socket_address >>= fun () ->
      let a2 = Lwt_unix.getsockname socket in
      let peer = Lwt_unix.getpeername socket in
      begin
	if (a2 = peer) then
	  Llio.lwt_failfmt "a socket should not connect to itself"
	else
	Lwt.return ()
      end >>= fun () ->
      let fd_field = Obj.field (Obj.repr socket) 0 in
      let (fdi:int) = Obj.magic (fd_field) in
      Lwt_log.info_f "__open_connection SUCCEEDED (fd=%i) %s %s" fdi
	(a2s a2) (a2s peer)
      >>= fun () ->
      let oc = Lwt_io.of_fd ~mode:Lwt_io.output socket in
      let ic = Lwt_io.of_fd ~mode:Lwt_io.input  socket in
      Lwt.return (ic,oc))
    (fun exn -> Lwt_log.info ~exn "__open_connection failed" >>= fun () ->
      Lwt_unix.close socket;
      Lwt.fail exn)



type connection = Lwt_io.input_channel * Lwt_io.output_channel
type mq = (Message.t * id) LWTQ.t

class tcp_messaging my_address =
  let never () = false in

object(self : # messaging )
  val _id2address = Hashtbl.create 10
  val _connections = Hashtbl.create 10
  val _connections_lock = Lwt_mutex.create ()
  val _qs = Hashtbl.create 10
  val _outgoing = LWTQ.create ()

  method register_receivers mapping =
    List.iter
      (fun (id,address) -> Hashtbl.add _id2address id address) mapping

  method private _get_target_address ~target =
    try 
      Some (Hashtbl.find _id2address target)
    with Not_found -> None


  method send_message m ~source ~target =
    LWTQ.add (source, target, m) _outgoing


  method get_buffer target =
    if Hashtbl.mem _id2address target then
      try Hashtbl.find _qs target
      with | Not_found ->
	begin
	  let tq = 
	    let capacity = Some 1000
	    and leaky = true in 
	    Lwt_buffer.create ~capacity ~leaky () in
	  let () = Hashtbl.add _qs target tq in
          tq
	end
    else
      failwith (Printf.sprintf "we don't know %s (it was never registered)" target)

  method recv_message ~target =
    let q = self # get_buffer target in Lwt_buffer.take q

  method private _establish_connection address =
    let host_ip, port = address in
    let socket_address = Network.make_address host_ip port in
    Lwt_log.debug_f "establishing connection to (%s,%i)" host_ip port
    >>= fun () ->
    Backoff.backoff (fun () -> __open_connection socket_address)
    >>= fun (ic,oc) ->
    let my_ip, my_port = my_address in
    Llio.output_string oc my_ip >>= fun () ->
    Llio.output_int oc my_port  >>= fun () ->
    Lwt.return (ic,oc)
      (* open_connection can also fail with Unix.Unix_error (63, "connect",_)
	 on local host *)

  method private _get_connection address =
    try
      let conn =  Hashtbl.find _connections address in
      Lwt.return conn
    with Not_found ->
      self # _establish_connection address >>= fun conn ->
      Hashtbl.add _connections address conn;
      Lwt.return conn

  method private _drop_connection address =

    Lwt_log.debug "_drop_connection" >>= fun () ->
    if Hashtbl.mem _connections address then
      begin
	let conn = Hashtbl.find _connections address in
	Lwt_log.debug "found connection, closing it" >>= fun () ->
	let ic,oc = conn in
      (* something with conn *)
	Lwt.catch
	  (fun () ->
	    Lwt_io.close ic >>= fun () ->
	    Lwt_io.close oc >>= fun () ->
	    Lwt_log.debug "closed connection"
	  )
	  (fun exn -> Lwt_log.warning ~exn "exception while closing, too little too late" )
	>>= fun () ->
	let () = Hashtbl.remove _connections address in
	Lwt.return ()
      end
    else
      begin
	let h,p = address in
	Lwt_log.debug_f "connection to (%s,%i) not found. we never had one..." h p
      end






  method private _pickle source target msg =
    let buffer = Buffer.create 40 in
    let () = Llio.string_to buffer source in
    let () = Llio.string_to buffer target in
    let () = Message.to_buffer msg buffer in
    Buffer.contents buffer


  method private _maybe_insert_connection address =
    let host,port = address in
    if Hashtbl.mem _connections address then
      Lwt_log.debug_f "XXX already have connection with (%S,%i)" host port
    else
      Lwt_log.debug_f "XXX first connection with (%S,%i)" host port

  method run ?(stop=never) () =
    Lwt_log.info "starting TCP_MESSAGING" >>= fun () ->
    let conditionally f =
      let b = stop () in
      if b then
	begin
	  Lwt_log.info "ending loop" >>= fun () ->
	  Lwt.return ()
	end
      else f ()
    in
    let protocol (ic,oc) =
      Llio.input_string ic >>= fun ip ->
      Llio.input_int ic    >>= fun port ->
      self # _maybe_insert_connection (ip,port) >>= fun () ->
      begin
	let rec loop () =
	  begin
	    Llio.input_int ic >>= fun msg_size ->
	    let buffer = String.create msg_size in
	    Lwt_io.read_into_exactly ic buffer 0 msg_size >>= fun () ->
	    let (source:id), pos1 = Llio.string_from buffer 0 in
	    let target, pos2 = Llio.string_from buffer pos1 in
	    let msg, _   = Message.from_buffer buffer pos2 in
	    Lwt_log.debug_f "tcp_messaging received msg from %s" source 
	    >>= fun () ->
	    let q = self # get_buffer target in
	    Lwt_buffer.add (msg, source) q >>=  fun () ->
	    conditionally loop
	  end
	in
	catch
	  (fun () -> loop ())
	  (fun exn ->
	    Lwt_log.info ~exn "going to drop outgoing connection as well" >>= fun () ->
	    let address = (ip,port) in
	    self # _drop_connection address >>= fun () ->
	    Lwt.fail exn)

      end
    in
    let rec sender_loop () =
      LWTQ.take _outgoing >>= fun (source, target, msg) ->
      (* Lwt_log.debug_f "sender_loop got %S" (Message.string_of msg) >>= fun () -> *)
      let ao = self # _get_target_address ~target in
      begin
	match ao with
	  | Some address ->
	    let try_send () =
	      self # _get_connection address >>= fun connection ->
	      let ic,oc = connection in
	      let pickled = self # _pickle source target msg in
              Llio.output_string oc pickled >>= fun () ->
	      Lwt_io.flush oc
	    in
	    Lwt.catch
	      (fun () -> try_send ())
	      (function
		| Unix.Unix_error(Unix.EPIPE,_,_) -> (* stale connection *)
		  begin
		    Lwt_log.debug_f "stale connection" >>= fun () ->
		    self # _drop_connection address >>= fun () ->
		    Lwt.catch
		      (fun () -> try_send ())
		      (fun exn -> Lwt_log.info_f ~exn "dropped message")
		  end
                | Lwt.Canceled ->
                    Lwt.fail Lwt.Canceled
		| exn ->
		  begin
		    Lwt_log.info_f ~exn
		      "dropping message %s with destination '%s' because of"
		      (Message.string_of msg) target >>= fun () ->
		    self # _drop_connection address >>= fun () ->
		    Lwt_log.debug "end of connection epilogue"
		  end
	      )
	  | None -> (* we don't talk to strangers *)
	    Lwt_log.warning_f "we don't send messages to %s (we don't know her)" target
      end
      >>= fun () -> conditionally sender_loop
	
    in
    let my_host, my_port = my_address in
    let server_t = Server.make_server_thread my_host my_port protocol
    in
    Lwt.pick [server_t ();sender_loop ();] >>= fun () ->
    Lwt_log.info "end of tcp_messaging"    >>= fun () ->
    Lwt.return ()


end
