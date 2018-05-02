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

open Messaging
open Message.Message
open Tcp_messaging
open Lwt
open OUnit

class player id (m:messaging) =
  let make_msg kind i = create kind (string_of_int i) in
  object(self)
    val mutable _lowest = None

    method _send target msg =
      Logger.debug_f_ "%s sending message %s to %s"
        id (string_of msg) target >>= fun () ->
      m # send_message msg ~source:id ~target:target

    method serve ?(n=100) ?(lowest=0) opp =
      let msg = make_msg "ping" n in
      m # send_message msg ~source:"a" ~target:opp >>=
      self # run lowest

    method multi_serve n targets =
      let msg = make_msg "ping" n in
      self # mcast_msg targets msg >>= fun () ->
      self # wait_for_response n targets

    method wait_for_response n  targets=
      m # recv_message ~target:id >>= fun (msg, _source) ->
      let n' = int_of_string ( msg.payload ) in
      Logger.debug_f_ "n=%d n'=%d" n n' >>= fun () ->
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
          m # recv_message ~target:id  >>= fun (msg, source) ->
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

let make_transport addresses =
  let tcp_transport =
    new tcp_messaging
        addresses "yummy" (fun _ _ _-> false) Node_cfg.default_max_buffer_size ~stop:(ref false)
        ~tcp_keepalive:Tcp_keepalive.default_tcp_keepalive
  in
  (tcp_transport :> messaging)

let eventually_die ?(t=10.0) () =
  Lwt_unix.sleep t >>= fun () ->
  let msg = Printf.sprintf "test takes too long (> %2f)" t in
  OUnit.assert_failure msg

let test_pingpong_1x1 () =
  let port = 40010 in
  let ip = "127.0.0.1" in
  let addresses = ["127.0.0.1"], port in
  let transport = make_transport addresses in
  let mapping = [("a", (ip,port));] in
  let () = transport # register_receivers mapping in
  let player_a = new player "a" transport in
  let cvar = Lwt_condition.create () in
  let td = Lwt_mvar.create_empty () in
  let setup_callback () = Lwt_condition.broadcast cvar (); Lwt.return () in
  let teardown_callback () = Lwt_mvar.put td () in
  let main_t () =
    Lwt.pick [ transport # run ~setup_callback ~teardown_callback ();
               begin
                 Lwt_condition.wait cvar >>= fun () ->
                 Logger.debug_ "going to serve" >>= fun () ->
                 player_a # serve "a"
               end;
               eventually_die ()
             ] >>= fun () -> Lwt_mvar.take td >>= fun () ->
    Logger.info_ "end of scenario"
  in
  Lwt_main.run (main_t());;

let test_pingpong_2x2 () =
  let port_a = 40010
  and port_b = 40020
  and local = "127.0.0.1" in
  let addresses_a = ([local], port_a)
  and addresses_b = ([local], port_b)
  in
  let transport_a = make_transport addresses_a in
  let transport_b = make_transport addresses_b in
  let m_tda = Lwt_mvar.create_empty () in
  let m_tdb = Lwt_mvar.create_empty () in
  let tda () = Lwt_mvar.put m_tda () in
  let tdb () = Lwt_mvar.put m_tdb () in
  let mapping = [("a", (local,port_a));
                 ("b", (local,port_b));] in
  let () = transport_a # register_receivers mapping in
  let () = transport_b # register_receivers mapping in
  let player_a = new player "a" transport_a in
  let player_b = new player "b" transport_b in
  let main_t () =
    Lwt.pick [ transport_a # run ~teardown_callback:tda ();
               transport_b # run ~teardown_callback:tdb ();
               player_a # serve "b";
               player_b # run 0 ();
               eventually_die ()
             ] >>= fun () ->
    Lwt_mvar.take m_tda >>= fun () ->
    Lwt_mvar.take m_tdb >>= fun () ->
    Logger.info_ "end of scenario"
  in
  Lwt_main.run (main_t())

let test_pingpong_multi_server () =
  let port_a = 40010
  and port_b = 40020
  and port_c = 40030
  and local = "127.0.0.1" in
  let addresses_a = ([local], port_a)
  and addresses_b = ([local], port_b)
  and addresses_c = ([local], port_c)
  in
  let transport_a = make_transport addresses_a in
  let transport_b = make_transport addresses_b in
  let transport_c = make_transport addresses_c in
  let mapping = [("a", (local, port_a));
                 ("b", (local, port_b));
                 ("c", (local, port_c))] in
  let () = transport_a # register_receivers mapping in
  let () = transport_b # register_receivers mapping in
  let () = transport_c # register_receivers mapping in
  let player_a = new player "a" transport_a in
  let player_b = new player "b" transport_b in
  let player_c = new player "c" transport_c in
  let m_tda = Lwt_mvar.create_empty () in
  let m_tdb = Lwt_mvar.create_empty () in
  let m_tdc = Lwt_mvar.create_empty () in
  let tda () = Lwt_mvar.put m_tda () in
  let tdb () = Lwt_mvar.put m_tdb () in
  let tdc () = Lwt_mvar.put m_tdc () in
  let timeout = 60.0 in
  let main_t () =
    Lwt.pick [ transport_a # run ~teardown_callback:tda();
               transport_b # run ~teardown_callback:tdb();
               transport_c # run ~teardown_callback:tdc();
               player_a # multi_serve 10000 ["b"; "c" ] ;
               player_b # run 0 ();
               player_c # play_dead ();
               eventually_die ~t:timeout ()
             ]
    >>= fun () ->
    Lwt_mvar.take m_tda >>= fun () ->
    Lwt_mvar.take m_tdb >>= fun () ->
    Lwt_mvar.take m_tdc >>= fun () ->
    Logger.info_ "end of scenario"
  in
  Lwt_main.run (main_t());;


let test_pingpong_restart () =
  let port_a = 40010
  and port_b = 40020
  and local = "127.0.0.1" in
  let address_a = ([local], port_a)
  and address_b = ([local], port_b)
  in
  let t_a = make_transport address_a in
  let t_b = make_transport address_b in
  let mapping = [("a", (local, port_a));
                 ("b", (local, port_b));] in
  let () = t_a # register_receivers mapping in
  let () = t_b # register_receivers mapping in
  let player_a = new player "a" t_a in
  let player_b = new player "b" t_b in
  let main_t =
    Lwt.pick [
      begin
        Lwt.pick [ t_a # run ();
                   player_a # run 50 () >>= fun () -> Logger.debug_ "a done"
                 ] >>= fun () ->
        Lwt_unix.sleep 1.0 >>= fun () ->
        let t_a' = make_transport address_a in
        let () = t_a' # register_receivers mapping in
        let player_a' = new player "a" t_a' in
        Lwt.pick [
          (Logger.info_ "new network" >>= fun () ->
           t_a' # run ());
          begin
            Logger.debug_ "a' will be serving momentarily" >>= fun () ->
            player_a' # serve ~n:200 "b"
          end
        ]
      end;
      t_b # run ();
      player_a # serve "b";
      player_b # run 0 ();
      eventually_die () ]
    >>= fun () -> Logger.info_ "main_t: after pick"
  in
  Lwt_main.run main_t

let suite = "tcp" >::: [
    "pingpong_1x1" >:: test_pingpong_1x1;
    "pingpong_2x2" >:: test_pingpong_2x2;
    "pingpong_multi_server" >:: test_pingpong_multi_server;
    "pingpong_restart" >:: test_pingpong_restart;
  ]
