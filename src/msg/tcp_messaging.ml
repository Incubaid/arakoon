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
open Network


type connection = Lwt_io.input_channel * Lwt_io.output_channel
type mq = (Message.t * id) LWTQ.t

let never () = Lwt.return false 
let no_callback () = Lwt.return ()

type drop_function = Message.t -> string -> string -> bool

class tcp_messaging my_address my_cookie (drop_it: drop_function) =
  let _MAGIC = 0xB0BAFE7L in
  let _VERSION = 1 in
  let my_host, my_port = my_address in
  let me = Printf.sprintf "(%s,%i)" my_host my_port 
  in
object(self : # messaging )
  val _id2address = Hashtbl.create 10
  val _connections = Hashtbl.create 10
  val _connections_lock = Lwt_mutex.create ()
  val _qs = Hashtbl.create 10
  val _outgoing = Hashtbl.create 10 
  val mutable _running = false
  val _running_c = Lwt_condition.create ()
  val mutable _my_threads = []

  method register_receivers mapping =
    List.iter
      (fun (id,address) -> Hashtbl.add _id2address id address) mapping

  method private _get_target_address ~target =
    try Some (Hashtbl.find _id2address target)
    with Not_found -> None


  method expect_reachable ~target = 
    match self # _get_target_address ~target with
      | None -> false
      | Some address -> Hashtbl.mem _connections address
	
	
  method private __die__ () = 
    Lwt_log.debug_f "tcp_messaging %s: cancelling my threads" me >>= fun () ->
    List.iter (fun w -> Lwt.cancel w) _my_threads;
    _my_threads <- [];
    Lwt.return () 

  method private _get_send_q ~target = 
    try Hashtbl.find _outgoing target 
    with Not_found -> 
      let capacity = Some 1000 in
      let leaky = true in
      let fresh = Lwt_buffer.create ~capacity ~leaky () in
      let loop = self # _make_sender_loop target fresh in
      Lwt.ignore_result (loop ());
      let () = Hashtbl.add _outgoing target fresh in
      fresh
	
  method send_message m ~source ~target =
    let tq = self # _get_send_q ~target in
    Lwt_buffer.add (source, target, m) tq 



  method private get_buffer (target:id) =   
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

  method recv_message ~target =
    let q = self # get_buffer target in 
    Lwt_buffer.take q

  method private _establish_connection address =
    let host_ip, port = address in
    let socket_address = Network.make_address host_ip port in
    Lwt_log.debug_f "establishing connection to (%s,%i)" host_ip port
    >>= fun () ->
    Lwt.pick[ Lwt_unix.timeout 5.0;
	      __open_connection socket_address] 
    >>= fun (ic,oc) ->
    let my_ip, my_port = my_address in
    Llio.output_int64 oc _MAGIC >>= fun () ->
    Llio.output_int oc _VERSION >>= fun () ->
    Llio.output_string oc my_cookie >>= fun () ->
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

  method private _make_sender_loop target target_q =
    let rec _loop_for_q () = 
      Lwt_buffer.take target_q >>= fun (source, target, msg) ->
      let ao = self # _get_target_address ~target in
      begin
	match ao with
	  | Some address ->
	    let drop exn reason =
	      Lwt_log.debug ~exn reason >>= fun () ->
	      self # _drop_connection address
	    in
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
		      (fun exn -> Lwt_log.debug_f ~exn "dropped message for %s" target)
		  end
                | Lwt.Canceled -> Lwt.fail Lwt.Canceled
		| Unix.Unix_error(Unix.ECONNREFUSED,_,_) as exn -> 
		  begin 
		    let reason = 
		      Printf.sprintf "machine with %s up, server down => dropping %s"
			target (Message.string_of msg ) 
		    in 
		    drop exn reason >>= fun () ->
		    Lwt_unix.sleep 1.0 (* not to hammer machine *)
		  end
		| Unix.Unix_error(Unix.EHOSTUNREACH,_,_) as exn ->
		  begin
		    let reason = 
		      Printf.sprintf "machine with %s unreachable => dropping %s"
			target (Message.string_of msg) 
		    in
		    drop exn reason >>= fun () ->
		    Lwt_unix.sleep 2.0
		  end
		| Lwt_unix.Timeout as exn ->
		  begin
		    let reason =
		      Printf.sprintf "machine with %s (probably) down => dropping %s"
			target (Message.string_of msg) 
		    in
		    drop exn reason >>= fun () ->
		    Lwt_unix.sleep 2.0 (* takes at least 2.0s to get up ;) *)
		  end
		| exn -> 
		  begin
		    let reason = 
		      Printf.sprintf "dropping message %s with destination '%s' because of"
			(Message.string_of msg) target 
		    in
		    drop exn reason
		  end
	      )
	  | None -> (* we don't talk to strangers *)
	    Lwt_log.warning_f "we don't send messages to %s (we don't know her)" target
      end
      >>= fun () -> _loop_for_q ()
    in
    let (w,u) = Lwt.task () in
    let thread () =
      w >>= fun () ->
      Lwt_log.debug "wait until tcp_messaging is running" >>= fun () ->
      begin
	if _running 
	then Lwt.return () 
	else Lwt_condition.wait _running_c 
      end 
      >>= fun () ->
      Lwt_log.debug_f "sender_loop for '%s' running" target >>= fun () ->
      Lwt.finalize 
	_loop_for_q 
	(fun () -> Lwt_log.debug_f "end of sender_q for '%s'" target)
    in 
    let () = _my_threads <- w :: _my_threads in
    Lwt.wakeup u ();
    thread




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
    else Lwt.return ()

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

  method run ?(up_and_running=no_callback)  () =
    Lwt_log.info_f "tcp_messaging %s: run" me >>= fun () ->
    let _check_mv magic version = 
	if magic = _MAGIC && version = _VERSION then Lwt.return () 
	else Llio.lwt_failfmt "MAGIC %Lx or VERSION %x mismatch" magic version
    in
    let _check_cookie cookie =
      if cookie <> my_cookie 
      then Llio.lwt_failfmt "COOKIE %s mismatch" cookie
      else Lwt.return ()
    in
    let protocol (ic,oc) =
      Llio.input_int64 ic >>= fun magic ->
      Llio.input_int ic >>= fun version ->
      _check_mv magic version >>= fun () ->
      Llio.input_string ic >>= fun cookie ->
      _check_cookie cookie >>= fun () ->
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
	    Lwt_log.debug_f "message from %s for %s" source target >>= fun () ->
	    if drop_it msg source target then loop () 
	    else
	      begin
		begin
		  if not (Hashtbl.mem _id2address source)
		  then let () = Hashtbl.add _id2address source (ip,port) in
		       Lwt_log.debug_f "registered %s => (%s,%i)" source ip port 
		  else Lwt.return () 
		end >>= fun () ->
		let q = self # get_buffer target in
		Lwt_buffer.add (msg, source) q >>=  fun () ->
		loop ()
	      end
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
    let server_t = Server.make_server_thread 
      ~setup_callback:up_and_running
      my_host my_port protocol
    in
    _running <- true;
    Lwt_condition.broadcast _running_c ();
    server_t () >>= fun () ->
    Lwt_log.info_f "tcp_messaging %s: end of run" me >>= fun () ->
    Lwt.return ()
    	
end
