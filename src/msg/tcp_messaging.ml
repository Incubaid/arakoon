(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

open Message
open Messaging
open Lwt
open Lwt_buffer
open Network

type connection = Lwt_unix.file_descr * Lwt_io.input_channel * Lwt_io.output_channel

let section =
  let s = Logger.Section.make "tcp_messaging" in
  let () = Logger.Section.set_level s Logger.Debug in
  s


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

let run_report connections =
  let rec loop () =
    let go = Hashtbl.fold (fun (addr, port) (socket, _, _) rest -> fun () ->
      begin try
        let info = Tcp_info.tcp_info (Lwt_unix.unix_file_descr socket) in
        Logger.info_f_ "TCP info for %s:%d: %s" addr port (Tcp_info.to_string info)
      with exn ->
        Logger.error_f_ ~exn "TCP info lookup for %s:%d failed" addr port
      end >>=
      rest)
      connections
      Lwt.return
    in
    go () >>= fun () ->
    Lwt_unix.sleep 60.0 >>=
    loop
  in
  loop ()

class tcp_messaging
  ?(timeout=60.0)
  ?(client_ssl_context:[> `Client ] Typed_ssl.t option)
  my_addresses my_cookie (drop_it: drop_function)
  max_buffer_size ~stop =

  let _MAGIC = 0x53E7965CL in
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

    method private _get_send_q ~target =
      try Hashtbl.find _outgoing target
      with Not_found ->
        let capacity = Some 20000 in
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
      let timeout = 5.0 in
      let ssl_context = client_ssl_context in
      Logger.info_f_ "establishing connection to (%s,%i) (timeout = %f)" host_ip port timeout
      >>= fun () ->
      Lwt_unix.with_timeout timeout
        (fun () ->__open_connection ?ssl_context socket_address)
      >>= fun (socket, ic, oc) ->
      Llio.output_int64 oc _MAGIC >>= fun () ->
      Llio.output_int oc _VERSION >>= fun () ->
      Llio.output_string oc my_cookie >>= fun () ->
      Llio.output_string oc my_ip >>= fun () ->
      Llio.output_int oc my_port  >>= fun () ->
      Lwt.return (socket, ic, oc)
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
                Lwt_unix.with_timeout
                  timeout
                  (fun () ->
                   self # _get_connection addresses >>= fun connection ->
                   let _, _ic, oc = connection in
                   let pickled = self # _pickle source target msg in
                   Llio.output_string oc pickled >>= fun () ->
                   Lwt_io.flush oc)
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
          let _, ic, oc = conn in
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

    method run ?(setup_callback=no_callback) ?(teardown_callback=no_callback) ?ssl_context () =
      Logger.info_f_ "tcp_messaging %s: run" me >>= fun () ->
      let _check_mv magic version =
        if magic = _MAGIC && version = _VERSION
        then Lwt.return ()
        else Llio.lwt_failfmt "MAGIC %Lx or VERSION %x mismatch" magic version
      in
      let _check_cookie cookie =
        if cookie <> my_cookie
        then Llio.lwt_failfmt "COOKIE %s mismatch" cookie
        else Lwt.return ()
      in
      let read_prologue ic =
        Llio.input_int64 ic >>= fun magic ->
        Llio.input_int ic >>= fun version ->
        _check_mv magic version >>= fun () ->
        Llio.input_string ic >>= fun cookie ->
        _check_cookie cookie >>= fun () ->
        Llio.input_string ic >>= fun ip ->
        Llio.input_int ic    >>= fun port ->
        let address = (ip,port) in
        self # _maybe_insert_connection address >>= fun () ->
        Lwt.return address
      in
      let next_message b0 ((ip, port) as address) ic =
        Llio.input_int ic >>= fun msg_size ->
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
        let buffer = Llio.make_buffer b1 0 in
        let (source:id) = Llio.string_from buffer in
        let target      = Llio.string_from buffer in
        let msg         = Message.from_buffer buffer in
        (*log_f "message from %s for %s" source target >>= fun () ->*)
        if drop_it msg source target then Lwt.return b1
        else
          begin
            begin
              if not (Hashtbl.mem _id2address source)
              then
                let () = self # _register_receiver source address in
                Logger.debug_f_ "registered %s => (%s,%i)" source ip port
              else Lwt.return ()
            end >>= fun () ->
            let q = self # get_buffer target in
            Lwt_buffer.add (msg, source) q >>=  fun () ->
            Lwt.return b1
          end
      in
      let die_after timeout address=
        Lwt_unix.sleep timeout >>= fun () ->
        let (ip,port) = address in
        Llio.lwt_failfmt
          "connection from (%s,%i) was idle too long (> %f)"
          ip port timeout
      in
      let protocol (ic,_oc,_cid) =
        Lwt_unix.with_timeout timeout (fun () -> read_prologue ic)
        >>= fun address ->
        let rec loop b0 =
          begin
            Lwt.pick [
              die_after timeout address;
              next_message b0 address ic]
            >>= fun b1 ->
            loop b1
          end
        in
        Lwt.catch
          (fun () -> loop (String.create 1024))
          (fun exn ->
             Logger.info_f_ ~exn "going to drop outgoing connection as well" >>= fun () ->
             self # _drop_connection address >>= fun () ->
             Lwt.fail exn)
      in
      let servers_t () =
        let start ip  =
          let name = Printf.sprintf "messaging_%s" ip in
          let scheme = Server.make_default_scheme () in
          let s = Server.make_server_thread
                    ~name ~setup_callback
                    ~teardown_callback ip my_port protocol
                    ~scheme ?ssl_context
                    ~stop
          in
          s ()
        in
        let sts = List.map start my_ips in
        Lwt.join sts
      in
      _running <- true;
      _my_threads <- (run_report _connections) :: _my_threads;
      Lwt_condition.broadcast _running_c ();
      servers_t () >>= fun () ->
      Logger.info_f_ "tcp_messaging %s: end of run" me >>= fun () ->
      Lwt.return ()

  end
