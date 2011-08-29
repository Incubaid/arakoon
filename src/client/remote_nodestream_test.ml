open Lwt
open OUnit
open Node_cfg
open Master_type
open Test_backend
open Remote_nodestream
open Arakoon_remote_client

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
  let server = 
    Server.make_server_thread ~setup_callback "127.0.0.1" port 
      (Client_protocol.protocol backend) in
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

let set_range port () = 
  let conversation (ic,oc) = 
    Lwt_log.debug "starting set_range ..." >>= fun () ->
    Lwt_unix.sleep 4.0 
  in
  __wrap__ port conversation


let get_tail port ()= 
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
    Lwt_log.debug "starting get_tail" >>= fun () ->
    ns # get_tail "k" >>= fun kvs -> 
    let got = List.length kvs in
    Lwt_log.debug_f "got: %i" got >>= fun () ->
    Lwt_io.close ic >>= fun () ->
    Lwt_io.close oc >>= fun () ->
    OUnit.assert_equal 3 got ~msg:"tail size does not match";
    Lwt.return ()
  in
  __wrap__ port conversation

let suite = 
  let w f = Extra.lwt_bracket setup f teardown in
  "nursery" >:::
    ["set_range" >:: w (set_range 6666);
     "get_tail"  >:: w (get_tail  5555);
    ]

