open Lwt
open OUnit
open Node_cfg
open Master_type
open Test_backend
open Remote_nodestream
open Arakoon_remote_client
open Routing
open Interval

let setup port = Lwt.return port

let teardown () = Lwt.return ()


let _cluster = "baby1"

let __wrap__ port conversation = 
  let sleep, notifier = Lwt.wait () in
  let tb = new test_backend _cluster in
  let backend = (tb :> Backend.backend) in
  let setup_callback ()  = 
    Lwt_log.info "callback" >>= fun () ->
    Lwt.wakeup notifier ();
    Lwt.return ()
  in
  let scheme = Server.make_default_scheme () in
  let server = 
    Server.make_server_thread ~setup_callback "127.0.0.1" port ~scheme
      (Client_protocol.protocol backend false) in
  let client_t () =
    sleep >>= fun () ->
    let address = Unix.ADDR_INET (Unix.inet_addr_loopback, port) in
    Lwt_io.open_connection address >>= function (ic,oc) ->
      conversation (ic,oc) >>= fun () ->
      Lwt_log.debug "end_of_senario" >>= fun () ->
      Lwt_io.close ic >>= fun () ->
      Lwt_io.close oc >>= fun () ->
      Lwt.return ()
  in
  Lwt.pick [client_t ();server ()] >>= fun () ->
  Lwt.return ()

let set_interval port () = 
  let conversation conn =
    Lwt_log.debug "starting set_interval ..." >>= fun () ->
    Common.prologue _cluster conn >>= fun () ->
    let i0 = Interval.make (Some "a") None None None in
    Lwt_log.debug_f "i0=%S" (Interval.to_string i0) >>= fun () ->
    Common.set_interval conn i0 >>= fun () -> 
    Common.get_interval conn >>= fun i1 ->
    OUnit.assert_equal ~printer:Interval.to_string i0 i1;
    Lwt_unix.sleep 4.0 
  in
  __wrap__ port conversation


let get_fringe port ()= 
  let fill_it_a_bit () =
    let address = Network.make_address "127.0.0.1" port in
    Lwt_io.with_connection address (fun conn ->
      make_remote_client _cluster conn >>= fun client ->
      Lwt_list.iter_s (fun (k,v) -> client # set k v) 
	[("k1", "vk1");
	 ("k2", "vk2");
	 ("p1", "vp1");
	 ("a" , "va");
	] >>= fun () ->
      client # get "k1" >>= fun v ->
      Lwt_log.debug_f "a[%s] = %s" "k1" v >>= fun () ->
      Lwt.return ()     
    )
  in
  let conversation conn = 
    fill_it_a_bit ()  >>= fun () ->
    let (ic,oc) = conn in 
    make_remote_nodestream _cluster conn >>= fun ns ->
    Lwt_log.debug "starting get_fringe" >>= fun () ->
    ns # get_fringe (Some "k") Routing.LOWER_BOUND >>= fun kvs -> 
    let got = List.length kvs in
    Lwt_log.debug_f "got: %i" got >>= fun () ->
    Lwt_io.close ic >>= fun () ->
    Lwt_io.close oc >>= fun () ->
    OUnit.assert_equal 3 got ~msg:"fringe size does not match";
    Lwt.return ()
  in
  __wrap__ port conversation


let set_route_delta port () =

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

let suite = 
  let w f = Extra.lwt_bracket setup f teardown in
  "nursery" >:::
    ["set_interval" >:: w (set_interval 6666);
     "get_fringe"  >:: w (get_fringe  5555);
     "set_routing"  >:: w (set_route_delta  4444);
    ]

