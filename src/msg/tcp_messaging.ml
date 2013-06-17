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
open Network

type connection = Lwt_io.input_channel * Lwt_io.output_channel

let section =
  let s = Logger.Section.make "tcp_messaging" in
  let () = Logger.Section.set_level s Logger.Debug in
  s


let never () = Lwt.return false 
let no_callback () = Lwt.return ()

type drop_function = Message.t -> string -> string -> bool
module RR = struct

  type t = { mutable addresses: address list }
  let create () = {addresses = [] } 
  let current t = List.hd t.addresses
  let rotate t = match t.addresses with
    | [] -> failwith "empty address list"
    | x :: xs -> t.addresses <- xs @ [x]
  let add t a = t.addresses <- a :: t.addresses

  let fold f a0 t = List.fold_left f a0 t.addresses
end


class tcp_messaging ?(timeout=60.0) my_addresses my_cookie (drop_it: drop_function) max_buffer_size =
  let _MAGIC = 0xB0BAFE7L in
  let _VERSION = 1 in
  let my_ips, my_port = my_addresses in
  let my_ip = List.hd my_ips in
  let me = Printf.sprintf "(%s,%i)" my_ip my_port
  in
object(self : # messaging )
  val _id2address = Hashtbl.create 10
  val _connections = (Hashtbl.create 10 : (address, connection) Hashtbl.t)
  val _connections_lock = Lwt_mutex.create ()
  val _qs = Hashtbl.create 10
  val _outgoing = Hashtbl.create 10 
  val mutable _running = false
  val _running_c = Lwt_condition.create ()
  val mutable _my_threads = []

  method private _register_receiver id address =
    let r = 
      try Hashtbl.find _id2address id 
      with Not_found ->
        let r = RR.create () in
        let () = Hashtbl.add _id2address id r in
        r
    in
    RR.add r address

  method register_receivers mapping = List.iter (fun (id,address) -> self # _register_receiver id address) mapping


  method private _get_target_addresses ~target =
    try Some (Hashtbl.find _id2address target)
    with Not_found -> None


  method expect_reachable ~target = 
    let addresses_o = self # _get_target_addresses ~target in
    match addresses_o with
      | None -> false
      | Some rr -> RR.fold (fun acc a -> acc || Hashtbl.mem _connections a) false rr

	
	
  method private __die__ () = 
    Logger.debug_f_ "tcp_messaging %s: cancelling my threads" me >>= fun () ->
    List.iter (fun w -> Lwt.cancel w) _my_threads;
    _my_threads <- [];
    Lwt.return () 

  method private _get_send_q ~target = 
    try Hashtbl.find _outgoing target 
    with Not_found -> 
      let capacity = Some 20000 in
      let leaky = true in
      let fresh = Lwt_buffer.create ~capacity ~leaky () in
      let loop = self # _make_sender_loop target fresh in
      Lwt_extra.ignore_result (loop ());
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
    let timeout = 5.0 in
    Logger.debug_f_ "establishing connection to (%s,%i) (timeout = %f)" host_ip port timeout
    >>= fun () ->
    Lwt.pick[ 
      Lwt_unix.timeout timeout;
	  __open_connection socket_address
    ] 
    >>= fun (ic,oc) ->    
    Llio.output_int64 oc _MAGIC >>= fun () ->
    Llio.output_int oc _VERSION >>= fun () ->
    Llio.output_string oc my_cookie >>= fun () ->
    Llio.output_string oc my_ip >>= fun () ->
    Llio.output_int oc my_port  >>= fun () ->
    Lwt.return (ic,oc)
     (* open_connection can also fail with Unix.Unix_error (63, "connect",_)
        on local host *)

  method private _get_connection (addresses:RR.t) =
    let (address:address) = RR.current addresses in
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
      let rr_o = self # _get_target_addresses ~target in
      begin
	match rr_o with
	  | Some (addresses:RR.t) ->
            let address = RR.current addresses in
	    let drop exn reason =
	      Logger.log ~exn section Logger.Debug reason >>= fun () ->
	      self # _drop_connection address
	    in
	    let try_send () =
	      self # _get_connection addresses >>= fun connection ->
	      let ic,oc = connection in
	      let pickled = self # _pickle source target msg in
	      Llio.output_string oc pickled >>= fun () ->
	      Lwt_io.flush oc
	    in
	    Lwt.catch
	      (fun () -> try_send ())
	      (fun exn ->
            let () = RR.rotate addresses in
            match exn with
		      | Unix.Unix_error(Unix.EPIPE,_,_) -> (* stale connection *)
		          begin
		            Logger.debug_ "stale connection" >>= fun () ->
		            self # _drop_connection address >>= fun () ->
		            Lwt.catch
		              (fun () -> try_send ())
		              (fun exn -> Logger.debug_f_ ~exn "dropped message for %s" target)
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
	      Logger.warning_f_ "we don't send messages to %s (we don't know her)" target
      end
      >>= fun () -> _loop_for_q ()
    in
    let (w,u) = Lwt.task () in
    let thread () =
      w >>= fun () ->
      Logger.debug_ "wait until tcp_messaging is running" >>= fun () ->
      begin
	    if _running 
	    then Lwt.return () 
	    else Lwt_condition.wait _running_c 
      end 
      >>= fun () ->
      Logger.debug_f_ "sender_loop for '%s' running" target >>= fun () ->
      Lwt.finalize 
	_loop_for_q 
	    (fun () -> Logger.debug_f_ "end of sender_q for '%s'" target)
    in 
    let () = _my_threads <- w :: _my_threads in
    Lwt.wakeup u ();
    thread




  method private _drop_connection address =

    Logger.debug_ "_drop_connection" >>= fun () ->
    if Hashtbl.mem _connections address then
      begin
	let conn = Hashtbl.find _connections address in
	Logger.debug_ "found connection, closing it" >>= fun () ->
	let ic,oc = conn in
      (* something with conn *)
	Lwt.catch
	  (fun () ->
	    Lwt_io.close ic >>= fun () ->
	    Lwt_io.close oc >>= fun () ->
	    Logger.debug_ "closed connection"
	  )
	  (fun exn -> Logger.warning_ ~exn "exception while closing, too little too late" )
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
      Logger.debug_f_ "XXX already have connection with (%S,%i)" host port
    else
      Logger.debug_f_ "XXX first connection with (%S,%i)" host port

  method run ?(setup_callback=no_callback) ?(teardown_callback=no_callback)  () =
    Logger.info_f_ "tcp_messaging %s: run" me >>= fun () ->
    let _check_mv magic version = 
	if magic = _MAGIC && version = _VERSION then Lwt.return () 
	else Llio.lwt_failfmt "MAGIC %Lx or VERSION %x mismatch" magic version
    in
    let _check_cookie cookie =
      if cookie <> my_cookie 
      then Llio.lwt_failfmt "COOKIE %s mismatch" cookie
      else Lwt.return ()
    in
    let protocol (ic,oc,cid) =
      Llio.input_int64 ic >>= fun magic ->
      Llio.input_int ic >>= fun version ->
      _check_mv magic version >>= fun () ->
      Llio.input_string ic >>= fun cookie ->
      _check_cookie cookie >>= fun () ->
      Llio.input_string ic >>= fun ip ->
      Llio.input_int ic    >>= fun port ->
      self # _maybe_insert_connection (ip,port) >>= fun () ->
      begin
	let rec loop b0 =
	  begin
        Lwt.pick [
          (Lwt_unix.sleep timeout >>= fun () -> 
           Llio.lwt_failfmt "connection from (%s,%i) was idle too long (> %f)" ip port timeout
          );
	      Llio.input_int ic] 
        >>= fun msg_size ->                      
        begin
          if msg_size > max_buffer_size
          then 
            let mbs_mb = max_buffer_size lsr 20 in 
            Llio.lwt_failfmt "msg_size (%i) > %iMB" msg_size mbs_mb
          else Lwt.return ()
        end
        >>= fun () ->
	    let b1 = 
	      if msg_size > String.length b0 
	      then String.create msg_size
	      else b0
	    in
	    Lwt_io.read_into_exactly ic b1 0 msg_size >>= fun () ->
	    let (source:id), pos1 = Llio.string_from b1 0 in
	    let target, pos2 = Llio.string_from b1 pos1 in
	    let msg, _   = Message.from_buffer b1 pos2 in
	    (*log_f "message from %s for %s" source target >>= fun () ->*)
	    if drop_it msg source target then loop b1
	    else
	      begin
		begin
		  if not (Hashtbl.mem _id2address source)
		  then 
                    let () = self # _register_receiver source (ip,port) in
		    Logger.debug_f_ "registered %s => (%s,%i)" source ip port 
		  else Lwt.return () 
		end >>= fun () ->
		let q = self # get_buffer target in
		Lwt_buffer.add (msg, source) q >>=  fun () ->
		loop b1
	      end
	  end
	in
	catch
	  (fun () -> loop (String.create 1024))
	  (fun exn ->
	    Logger.info_f_ ~exn "going to drop outgoing connection as well" >>= fun () ->
	    let address = (ip,port) in
	    self # _drop_connection address >>= fun () ->
	    Lwt.fail exn)

      end
    in
    let servers_t () = 
      let start ip  = 
        let name = Printf.sprintf "messaging_%s" ip in
        let scheme = Server.make_default_scheme () in
        let s = Server.make_server_thread ~name ~setup_callback 
          ~teardown_callback ip my_port protocol 
          ~scheme
        in
        s () 
      in
      let sts = List.map start my_ips in
      Lwt.join sts
    in
    _running <- true;
    Lwt_condition.broadcast _running_c ();
    servers_t () >>= fun () ->
    Logger.info_f_ "tcp_messaging %s: end of run" me >>= fun () ->
    Lwt.return ()
    	
end
