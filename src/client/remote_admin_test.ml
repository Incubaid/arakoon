open Mem_store
open Core
open Mp_driver
open Remote_admin

open Lwt
open Statistics
open Pq
open OUnit
open Routing

open Arakoon_client

module MyStore = (MemStore : STORE)
module MyDispatcher = Dispatcher.ADispatcher(MyStore)
module MyHandler = C.ProtocolHandler(MyStore) (MyDispatcher)
module MyDriver = MPDriver(MyDispatcher)

let _CLUSTER = "admin_test"
type real_test = admin -> client -> unit Lwt.t

let create_resyncs others cluster_id = Hashtbl.create 3

let create_store _ myname = 
  let ro = false in
  let fn = "whatever" in
  MyStore.create fn ro


let __client_server_wrapper__ cluster (real_test: real_test) = 
  let () = Lwt_log.Section.set_level Lwt_log.Section.main Lwt_log.Debug in
  let port = 7777 in
  let conversations conn1 conn2 = 
    Lwt_log.info "started conversations" >>= fun () ->
    Remote_admin.make cluster conn1 >>= fun admin ->
    Arakoon_remote_client.make_remote_client cluster conn2 >>= fun client ->
    real_test admin client >>= fun () -> 
    Lwt.return ()
  in
  let sleep, notifier = wait () in
  let td_var = Lwt_mvar.create_empty () in
  let setup_callback () = 
    Lwt_log.info "setup_callback" >>= fun () ->
    Lwt.wakeup notifier();
    Lwt.return ()
  in
  let me = "node_0" in
  let teardown_callback () = Lwt_mvar.put td_var () in
  let my_address = ("127.0.0.1", 7778) in
  let stats = Statistics.create () in 
  let resyncs = create_resyncs [] _CLUSTER in
  let timeout_q = PQ.create () in
  let drop_it _ _ _ = false in
  let messaging = new Tcp_messaging.tcp_messaging my_address _CLUSTER drop_it in
  let () = messaging # register_receivers [me, my_address] in
  let clients_t () = 
    let address = Unix.ADDR_INET (Unix.inet_addr_loopback, port) in
    Lwt_io.open_connection address >>= fun (ic1,oc1) ->
    Lwt_io.open_connection address >>= fun (ic2,oc2) ->
    conversations (ic1,oc1) (ic2,oc2) >>= fun () ->
    Lwt_log.debug "end_of_scenario" >>= fun () ->
    Lwt_io.close ic1 >>= fun () ->
    Lwt_io.close oc1 >>= fun () ->
    Lwt_io.close ic2 >>= fun () ->
    Lwt_io.close oc2 >>= fun () ->
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
      begin Lwt_unix.sleep 10.0 >>= fun () -> clients_t () end; 
    ] 
    >>= fun () -> 
    Lwt_mvar.take td_var  >>= fun () ->
    Lwt_log.info "server down"
  in

  Lwt_main.run main
    

let wrap f () = __client_server_wrapper__ _CLUSTER f


let _test_get_fringe (admin: admin) (client:client) = 
  Lwt_list.iter_s (fun (k,v) -> client # set k v) 
	[("k1", "vk1");
	 ("k2", "vk2");
	 ("p1", "vp1");
	 ("a" , "va");
	]
  >>= fun () ->
  client # get "k1" >>= fun v ->
  Lwt_log.debug_f "a[%s] = %s" "k1" v >>= fun () ->
  Lwt_log.debug "starting get_fringe" >>= fun () ->
  admin # get_fringe (Some "k") Routing.LOWER_BOUND >>= fun kvs -> 
  let got = List.length kvs in
  Lwt_log.debug_f "got: %i" got >>= fun () ->  
  Lwt_list.iter_s (fun (k,v) -> Lwt_log.debug_f "%s,%s" k v) kvs >>= fun () ->
  Lwt.return ()     

let map_wrap = List.map (fun (name, inner) -> name >:: (wrap inner))

let suite = "nursery" >:::
     map_wrap [
       ("get_fringe", _test_get_fringe);
     ]


(*
let set_interval () = 
  let conversation conn =
    Lwt_log.debug "starting set_interval ..." >>= fun () ->
    Common.prologue _CLUSTER conn >>= fun () ->
    let i0 = Interval.make (Some "a") None None None in
    Lwt_log.debug_f "i0=%S" (Interval.to_string i0) >>= fun () ->
    Common.set_interval conn i0 >>= fun () -> 
    Common.get_interval conn >>= fun i1 ->
    OUnit.assert_equal ~printer:Interval.to_string i0 i1;
    Lwt_unix.sleep 4.0 
  in
  __wrap__ port conversation


let get_fringe ()= 


let set_route_delta () =

  let repr = ["left","k";], "right" in
  let target_repr = ["left","l";], "right" in
  let r = Routing.build repr in
  let r_target = Routing.build target_repr in
  let old_ser = Buffer.create 15 in
  Routing.routing_to old_ser r_target ;
  let conversation conn =
    make_remote_nodestream _cluster conn >>= fun ns ->
    Lwt_log.debug "starting set_routing" >>= fun () ->
    ns # set_routing r >>= fun () ->
    Lwt_log.debug "starting set_routing_delta" >>= fun () -> 
    ns # set_routing_delta "left" "l" "right" >>= fun () ->
    Lwt_log.debug "starting get_routing" >>= fun () -> 
    ns # get_routing () >>= fun new_r ->
    let new_ser = Buffer.create 15 in
    Routing.routing_to new_ser new_r;
    let old_str = Buffer.contents old_ser in
    let new_str = Buffer.contents new_ser in
    Lwt_log.debug_f "old_str: %s " old_str >>= fun () ->
    Lwt_log.debug_f "new_str: %s" new_str  >>= fun () ->
    OUnit.assert_equal old_str new_str;
    Lwt.return ()  
  in
  __wrap__ port conversation

*)

