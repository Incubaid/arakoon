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

(*
open Baardskeerder 
open Unix
open Interval
*)

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

let _test_assert  (client: Arakoon_client.client) =
  client # set "key" "value" >>= fun () ->
  client # aSSert "key" (Some "value") >>= fun () ->
  Lwt.catch
    (fun () -> client # aSSert "key" (Some "nothing")  >>= fun () ->
      OUnit.assert_bool "should not get here" false; 
      Lwt.return ()
    )
    (function
      | Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED, "key") -> Lwt.return ()
      | ex -> Lwt.fail ex
    )
  >>= fun () ->
  Lwt_log.info "part-2" >>= fun () ->
  Lwt.catch
    (fun () -> client # aSSert "nothing" (Some "value")  >>= fun () ->
      OUnit.assert_bool "should not get here@part_2" false; 
      Lwt.return ()
    )
    (function
      | Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED, "nothing") -> Lwt.return ()
      | ex -> Lwt.fail ex
    )
    >>= fun () ->
  Lwt_log.info "part-3" >>= fun () ->
  client # delete "key" >>= fun () ->
  Lwt.catch
    (fun () -> client # aSSert "key" (Some "value")  >>= fun () ->
      OUnit.assert_bool "should not get here @part_3" false; 
      Lwt.return ()
    )
    (function
      | Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED, "key") -> Lwt.return ()
      | ex -> Lwt.fail ex
    )
  >>= fun () -> 
  Lwt.return ()    
      
let _test_assert_exists  (client: Arakoon_client.client) =
  client # set "key" "value" >>= fun () ->
  client # aSSert_exists "key" >>= fun () ->
  Lwt.catch
    (fun () -> client # aSSert_exists "key_wrong"  >>= fun () ->
      OUnit.assert_bool "should not get here" false; 
      Lwt.return ()
    )
    (function
      | Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED, "key_wrong") -> Lwt.return ()
      | ex -> Lwt.fail ex
    )
  >>= fun () ->
  Lwt_log.info "part-2" >>= fun () ->
  client # delete "key" >>= fun () ->
  Lwt.catch
    (fun () -> client # aSSert_exists "key"  >>= fun () ->
      OUnit.assert_bool "should not get here @part_2" false; 
      Lwt.return ()
    )
    (function
      | Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED, "key") -> Lwt.return ()
      | ex -> Lwt.fail ex
    )
  >>= fun () -> 
  Lwt.return () 

let _test_confirm (client: Arakoon_client.client) = 
  let key = "key" and value = "value" in
  let value2 = "value2" and value3 = "value3" in
  client # confirm key value >>= fun () ->
  client # get key >>= fun v2 ->
  OUnit.assert_equal v2 value;
  client # confirm key value >>= fun () ->
  client # get key >>= fun v3 ->
  OUnit.assert_equal v3 value;
  client # confirm key value2 >>= fun () ->
  client # get key >>= fun v4 ->
  OUnit.assert_equal v4 value2;
  client # set key value >>= fun () ->
  client # confirm key value2 >>= fun () ->
  client # get key >>= fun v5 ->
  OUnit.assert_equal v5 value2;
  client # delete key >>= fun () ->
  client # confirm key value3 >>= fun () ->
  client # get key >>= fun v6 ->
  OUnit.assert_equal v6 value3;
  Lwt.return ()

let _test_sequence (client: Arakoon_client.client) = 
  client # set "XXX0" "YYY0" >>= fun () ->
  let changes = [Arakoon_client.Set("XXX1","YYY1");
                 Arakoon_client.Set("XXX2","YYY2");
                 Arakoon_client.Set("XXX3","YYY3");
                 Arakoon_client.Assert("XXX3",(Some "YYY3"));
                 Arakoon_client.Assert_exists("XXX1");
                 Arakoon_client.Set("XXX3","YYY3");
                 Arakoon_client.Delete "XXX0";
                 Arakoon_client.Delete "XXX3";
                ]
  in
  client # sequence changes >>= fun () ->
  client # get "XXX1" >>= fun v1 ->
  OUnit.assert_equal v1 "YYY1";
  client # get "XXX2" >>= fun v2 ->
  OUnit.assert_equal v2 "YYY2";
  client # exists "XXX0" >>= fun exists ->
  OUnit.assert_bool "XXX0 should not be there" (not exists);
  client # exists "XXX3" >>= fun exists2 ->
  OUnit.assert_bool "XXX3 should not be there" (not exists2);
  Lwt.return ()

let _test_sequence_inside_sequence (client: Arakoon_client.client) = 
  let changes = [Arakoon_client.Set("XXX1","YYY1");
                 Arakoon_client.Set("XXX2","YYY2");
                 Arakoon_client.Set("XXX3","YYY3");
                ]
  in
  let changes2 = [
                  Arakoon_client.Sequence changes;
                  Arakoon_client.Delete "XXX3";
                ]
  in
  client # sequence changes2 >>= fun () ->
  client # get "XXX1" >>= fun v1 ->
  OUnit.assert_equal v1 "YYY1";
  client # get "XXX2" >>= fun v2 ->
  OUnit.assert_equal v2 "YYY2";
  client # exists "XXX3" >>= fun exists2 ->
  OUnit.assert_bool "XXX3 should not be there" (not exists2);
  Lwt.return ()


let _test_user_fn (client: Arakoon_client.client) =
    let myFn (ts:(Core.k, Core.v) Hashtbl.t) (vo) = 
    let () = Hashtbl.replace ts "user_fn" "user_fn" in
    Lwt.return None
  in
  let () = Userdb.Registry.register2 "myFn" myFn in
  client # user_function "myFn" (Some "user_fn") >>= fun ro ->
  client # get "user_fn" >>= fun res ->
  OUnit.assert_equal res "user_fn";
  Lwt.return ()
  
  
let wrap f = (fun () -> __client_server_wrapper__ _CLUSTER f)
  
let map_wrap = List.map (fun (name, inner) -> name >:: (wrap inner))

let suite = "remote_client" >::: 
  map_wrap [
    ("set_get", _test_set_get) ;
    ("set_delete", _test_set_delete);
    ("delete_non_existing", _test_delete_non_existing) ;
    ("assert" , _test_assert);
    ("assert_exists" , _test_assert_exists);
    ("assert_confirm" , _test_confirm);
    ("assert_sequence" , _test_sequence);
    ("assert_sequence_rec" , _test_sequence_inside_sequence);
    ("assert_user_fn"      , _test_user_fn);
  ]

