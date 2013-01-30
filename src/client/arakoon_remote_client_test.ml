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

open Core
open Mem_store
open Mp_driver
open Lwt
open Arakoon_remote_client
open Statistics
open Pq
open OUnit


module MyStore = (MemStore :STORE)
module MyDispatcher = Dispatcher.ADispatcher(MyStore)  
module MyHandler = C.ProtocolHandler(MyStore) (MyDispatcher)
module MyDriver = MPDriver(MyDispatcher)


let _CLUSTER = "sweety"

type real_test = Arakoon_client.client -> unit Lwt.t

let create_resyncs others cluster_id = Hashtbl.create 3 

let create_store _ myname =
  let ro = false in
  let fn = "whatever" in
  MyStore.create fn ro


let __client_server_wrapper__ cluster (real_test:real_test) =
  let () = Lwt_log.Section.set_level Lwt_log.Section.main Lwt_log.Debug in
  let port = 7777 in
  
  let conversation connection  =
    Lwt_log.info "started conversation" >>= fun () ->
    make_remote_client cluster connection 
    >>= fun (client:Arakoon_client.client) ->
    real_test client >>= fun () -> Lwt.return ()
  in
  let sleep, notifier = wait () in
  let td_var = Lwt_mvar.create_empty () in
  let setup_callback () =
    Lwt_log.info "setup_callback" >>= fun () ->
    Lwt.wakeup notifier ();
    Lwt.return ()
  in
  let me= "node_0" in
  let teardown_callback () = Lwt_mvar.put td_var () in
  let my_address = ("127.0.0.1", 7778) in
  let stats = Statistics.create () in
  let resyncs = create_resyncs [] _CLUSTER in
  let timeout_q = PQ.create () in 
  let drop_it _ _ _ = false in
  let messaging = new Tcp_messaging.tcp_messaging my_address _CLUSTER drop_it in
  let () = messaging # register_receivers [me, my_address] in

  let client_t () =
    let address = Unix.ADDR_INET (Unix.inet_addr_loopback, port) in
    Lwt_io.open_connection address >>= function (ic,oc) ->
      conversation (ic,oc) >>= fun () ->
      Lwt_log.debug "end_of_senario" >>= fun () ->
      Lwt_io.close ic >>= fun () ->
      Lwt_io.close oc >>= fun () ->
      Lwt.return ()
  in
  let lease_period = 3.0 in  
  let build_start_state () = 
    let q = 1 in
    let n_others = [] in
    let ix = 0 in
    let node_cnt = 1 in
    let c = Mp.MULTI.build_mp_constants q me n_others lease_period ix node_cnt in
    let n = Core.NTickUtils.start_tick in
    let p = Core.ITickUtils.start_tick in
    let a = Core.ITickUtils.start_tick in
    let u = None in
    Mp.MULTI.build_state c n p a u 
  in
  let main =
    create_store () me >>= fun store ->
    let disp     = MyDispatcher.create messaging store timeout_q resyncs in
    let q        = PQ.create () in
    let driver   = MyDriver.create disp timeout_q q in
    let protocol = MyHandler.protocol me stats driver store in
    let s0 = build_start_state () in
    let delayed_timeout = Mp.MULTI.A_START_TIMER (s0.Mp.MULTI.round, 
                                                  Core.MTickUtils.start_tick, 
                                                  lease_period) 
    in
    let pass_msg msging q target = 
      let rec loop () =  
        msging # recv_message ~target >>= fun (msg_s, id) ->
        let msg = Mp.MULTI.msg_of_string msg_s in
        PQ.push q msg;
        loop ()
      in loop ()
    in
    MyDriver.dispatch driver s0 delayed_timeout >>= fun s1 ->
    let server () = 
      Server.make_server_thread 
        ~setup_callback 
        ~teardown_callback
        "127.0.0.1" port
        protocol ()
    in

    Lwt.pick [
      messaging # run();
      MyDriver.serve driver s1 None;
	  server ();
      pass_msg messaging timeout_q me;
      begin Lwt_unix.sleep 10.0 >>= fun () ->client_t () end; 
    ] 
    >>= fun () -> 
    Lwt_mvar.take td_var  >>= fun () ->
    Lwt_log.info "server down"
  in

  Lwt_main.run main


let _test_set_get (client: Arakoon_client.client) = 
    let v = "X" in
    client # set "x" v >>= fun () ->
    client # get "x" >>= fun v2 ->
    Lwtc.log "v = %S v2 = %S" v v2 >>= fun ()-> 
    OUnit.assert_equal ~printer:(fun s -> s) v v2;
    Lwt.return ()

let _test_set_delete (client: Arakoon_client.client) = 
  let v = "X" in
  client # set "x" v >>= fun () ->
  client # delete "x" >>= fun () ->
  Lwt.return ()

let _test_delete_non_existing (client:Arakoon_client.client) = 
  Lwt.catch
    (fun () -> client # delete "I'm not there"  >>= fun () ->
      OUnit.assert_bool "should not get here" false; 
      Lwt.return ()
    )
    (function
      | Arakoon_exc.Exception(Arakoon_exc.E_NOT_FOUND, "I'm not there") -> Lwt.return ()
      | ex -> Lwt.fail ex
    )
    
      
let wrap f = (fun () -> __client_server_wrapper__ _CLUSTER f)
  
let map_wrap = List.map (fun (name, inner) -> name >:: (wrap inner))

let suite = "remote_client" >::: 
  map_wrap [
    ("set_get", _test_set_get) ;
    ("set_delete", _test_set_delete);
    ("delete_non_existing", _test_delete_non_existing) ;
  ]

