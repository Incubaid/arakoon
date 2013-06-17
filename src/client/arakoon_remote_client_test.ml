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

open OUnit
open Lwt
open Log_extra
open Arakoon_remote_client
open Test_backend
open Update

let section = Logger.Section.main

let cucu s =
  Lwt_io.eprintlf "cucu: %s" s >>= fun () ->
  Lwt_io.flush Lwt_io.stderr

let _CLUSTER = "sweety"

type real_test = Arakoon_client.client -> unit Lwt.t

let __client_server_wrapper__ cluster (real_test:real_test) =
  let port = 7777 in
  let conversation connection  =
    cucu "started conversation" >>= fun () ->
    make_remote_client cluster connection 
    >>= fun (client:Arakoon_client.client) ->
    real_test client >>= fun () -> Lwt.return ()
  in
  let sleep, notifier = wait () in
  let td_var = Lwt_mvar.create_empty () in
  let setup_callback () =
    Logger.info_ "callback" >>= fun () ->
    Lwt.wakeup notifier ();
    Lwt.return ()
  in
  let teardown_callback () = Lwt_mvar.put td_var () in
  let tb = new test_backend _CLUSTER in
  let backend = (tb :> Backend.backend) in
  let scheme = Server.make_default_scheme () in
  let server = Server.make_server_thread 
    ~setup_callback 
    ~teardown_callback
    ~scheme
    "127.0.0.1" port
    (Client_protocol.protocol backend) in

  let client_t () =
    let address = Unix.ADDR_INET (Unix.inet_addr_loopback, port) in
    Lwt_io.open_connection address >>= function (ic,oc) ->
    conversation (ic,oc) >>= fun () ->
    Logger.debug_ "end_of_senario" >>= fun () ->
    Lwt_io.close ic >>= fun () ->
    Lwt_io.close oc >>= fun () ->
    Lwt.return ()
  in
  let main () =
    Lwt.pick [client_t ();
	      server ();] >>= fun () -> 
    Lwt_mvar.take td_var  >>= fun () ->
    Logger.info_ "server down"
  in
  Lwt_extra.run (main());;

let test_ping () =
  let real_test client =
    client # ping "test_ping" _CLUSTER >>= fun server_version ->
    OUnit.assert_equal server_version "test_backend.0.0.0";
    Lwt.return ()
  in __client_server_wrapper__ _CLUSTER real_test

let test_wrong_cluster () =
  let wrong_cluster = "mindy" in  
  let real_test client = 
    Lwt.catch 
      (fun () ->
	client # ping "boba fet" wrong_cluster >>= fun result ->
	OUnit.assert_bool "we should not be able to connect to this cluster" false;
	Lwt.return ())
      (fun exn -> Logger.debug_f_ ~exn "ok, this cluster is not %s" wrong_cluster
	>>= fun () -> Lwt.return ()) 
      >>= fun () ->
    Lwt.return () 
  in __client_server_wrapper__ wrong_cluster real_test

let test_set_get () =
  let real_test (client:Arakoon_client.client) =
    client # set "key" "value" >>= fun () ->
    client # get "key" >>= fun value ->
    OUnit.assert_equal value "value";
    Lwt.return ()
  in __client_server_wrapper__ _CLUSTER real_test

let test_assert_exists () =
  let real_test (client:Arakoon_client.client) =
    client # set "key" "value" >>= fun () ->
    client # aSSert_exists "key" >>= fun () ->
    Lwt.return ()
  in __client_server_wrapper__ _CLUSTER real_test

let test_confirm () =
  let real_test (client:Arakoon_client.client) = 
    let key = "key" and value = "value" in
    client # confirm key value >>= fun () ->
    client # get key >>= fun v2 ->
    OUnit.assert_equal v2 value;
    client # confirm key value >>= fun () ->
    client # get key >>= fun v3 ->
    OUnit.assert_equal v3 value;
    client # confirm key "something else" >>= fun () ->
    client # get key >>= fun v4 ->
    OUnit.assert_equal v4 "something else";
    Lwt.return ()
  in
  __client_server_wrapper__ _CLUSTER real_test

let test_delete () =
  let real_test (client:Arakoon_client.client) =
    client # set "key" "value" >>= fun () ->
    client # delete "key" >>= fun () ->
    Lwt.catch
      (fun () -> 
	client # get "key" >>= fun value ->
	Lwt.return ())
      (function
	| Arakoon_exc.Exception (Arakoon_exc.E_NOT_FOUND,_) -> 
	  Lwt_io.eprintlf "ok!"
	| exn ->
	  Logger.fatal_ ~exn "wrong exception" >>= fun () ->
	  OUnit.assert_failure 
	    "get of non_existing key does not throw correct exception"
      ) 
    >>= fun () ->
    Logger.info_ "part-2" >>= fun () ->
    Lwt.catch
      (fun () ->
	client # delete "key" >>= fun () -> 
	Logger.info_ "should not get here"
      )
      (function
	| Arakoon_exc.Exception (Arakoon_exc.E_NOT_FOUND, _) -> 
	  Lwt.return ()
	| exn -> Logger.fatal_ ~exn "should not be" >>= fun () ->
	  OUnit.assert_failure "XXX"
      )
      
  in __client_server_wrapper__ _CLUSTER real_test

let test_sequence () =
  let real_test (client:Arakoon_client.client) = 
    client # set "XXX0" "YYY0" >>= fun () ->
    let changes = [Arakoon_client.Set("XXX1","YYY1");
		   Arakoon_client.Set("XXX2","YYY2");
		   Arakoon_client.Set("XXX3","YYY3");
		   Arakoon_client.Delete "XXX0";
		  ]
    in
    client # sequence changes >>= fun () ->
    client # get "XXX1" >>= fun v1 ->
    OUnit.assert_equal v1 "YYY1";
    client # get "XXX2" >>= fun v2 ->
    OUnit.assert_equal v2 "YYY2";
    client # get "XXX3">>= fun v3 ->
    OUnit.assert_equal v3 "YYY3";
    client # exists "XXX0" >>= fun exists ->
    OUnit.assert_bool "XXX0 should not be there" (not exists);
    Lwt.return ()
  in 
  __client_server_wrapper__ _CLUSTER real_test


let _test_user_function (client:Arakoon_client.client) = 
  client # user_function "reverse" (Some "echo") >>= fun ro ->
  Logger.debug_f_ "we got %s" (string_option2s ro) >>= fun () ->
  OUnit.assert_equal ro  (Some "ohce");
  Lwt.return ()

let test_user_function () = 
  __client_server_wrapper__ _CLUSTER _test_user_function

let _clear (client:Arakoon_client.client)  () = 
  client # range None true None true 1000 >>= fun xn ->
  Lwt_list.iter_s (fun x -> client # delete x) xn

let _fill (client:Arakoon_client.client) max = 
  let rec loop n =
    let cat s i = Printf.sprintf "%s%03d" s i in
    if n = max then Lwt.return ()
    else
      client # set (cat "xey" n) (cat "value" n) >>= fun () ->
      loop (n+1)
  in
  loop 0

let _test_range (client:Arakoon_client.client) =
  _clear client () >>= fun () ->
  _fill client 100 >>= fun () ->
  client # range (Some "xey") true (Some "xey009") true 3 >>= fun xn ->
  let length = List.length xn in
  Logger.debug_f_ "received a list of size %i" length >>= fun () ->
  let msg = Printf.sprintf "expecting length: 3, actual: %d" length in
  let () = OUnit.assert_equal ~msg 3 length in
  Lwt_list.iter_s (fun x -> Logger.debug_f_ "key %s" x) xn >>= fun () ->
  let () = OUnit.assert_bool "0" (List.mem "xey000" xn) in
  let () = OUnit.assert_bool "1" (List.mem "xey001" xn) in
  let () = OUnit.assert_bool "2" (List.mem "xey002" xn) in
  client # range None true (Some "xey009") true 11 >>= fun xn ->
  (* let xn = List.filter (fun x -> x.[0] <> '@' && x.[1] <> '@') xn in *)
  let length = List.length xn in
  Lwt_list.iter_s (fun x -> Logger.debug_f_ "key %s" x) xn >>= fun () ->
  let msg = Printf.sprintf "expecting length: 10, actual: %d" length in
  let () = OUnit.assert_equal ~msg 10 length in
  let () = OUnit.assert_bool "x0" (List.mem "xey000" xn) in
  let () = OUnit.assert_bool "x1" (List.mem "xey001" xn) in
  let () = OUnit.assert_bool "x2" (List.mem "xey002" xn) in
  let () = OUnit.assert_bool "x3" (List.mem "xey003" xn) in
  let () = OUnit.assert_bool "x4" (List.mem "xey004" xn) in
  let () = OUnit.assert_bool "x5" (List.mem "xey005" xn) in
  let () = OUnit.assert_bool "x6" (List.mem "xey006" xn) in
  let () = OUnit.assert_bool "x7" (List.mem "xey007" xn) in
  let () = OUnit.assert_bool "x8" (List.mem "xey008" xn) in
  let () = OUnit.assert_bool "x9" (List.mem "xey009" xn) in
  client # range None true (Some "xey001") false 11 >>= fun xn ->
  (* let xn = List.filter (fun x -> x.[0] <> '@' && x.[1] <> '@') xn in *)
  let length = List.length xn in
  Lwt_list.iter_s (fun x -> Logger.debug_f_ "key %s" x) xn >>= fun () ->
  let msg = Printf.sprintf "expecting length: 1, actual: %d" length in
  let () = OUnit.assert_equal ~msg 1 length in
  let () = OUnit.assert_bool "x0" (List.mem "xey000" xn) in
  Lwt.return ()

let _test_reverse_range (client:Arakoon_client.client) = 
  _clear client () >>= fun () ->
  _fill client 100 >>= fun () ->
  client # rev_range_entries (Some "xey100") true (Some "xey009") true 3 >>= fun xn ->
  Lwt_list.iter_s (fun (k,v) -> Logger.debug_f_ "key %s" k) xn >>= fun () ->
  let k,_ = List.hd xn in 
  let () = OUnit.assert_bool "hd" (k  = "xey099") in
  Lwt.return ()

let test_range () = __client_server_wrapper__ _CLUSTER _test_range

let test_reverse_range () = __client_server_wrapper__ _CLUSTER _test_reverse_range

let _prefix_keys_test (client:Arakoon_client.client) =
  let cat s i = s ^ (string_of_int i) in
  client # set "foo" "bar" >>= fun () ->
  client # set "foo2" "bar2" >>= fun () ->
  let rec fill n =
    if n = 100 then Lwt.return ()
    else
      client # set (cat "key" n) (cat "value" n) >>= fun () ->
    fill (n+1)
  in
  fill 0 >>= fun () ->
  client # prefix_keys "ke" 1000 >>= fun list ->
  let length = List.length list in
  Logger.debug_f_ "received a list of size %i" length >>= fun () ->
  let () = OUnit.assert_equal 100 length in
  let () = OUnit.assert_bool "0" (List.mem "key0" list) in
  let () = OUnit.assert_bool "99" (List.mem "key99" list) in
  let () = OUnit.assert_bool "foo" (not (List.mem "foo" list)) in
  let () = OUnit.assert_bool "foo2" (not (List.mem "foo2" list)) in
  Lwt.return ()

let test_prefix_keys () =
  __client_server_wrapper__ _CLUSTER _prefix_keys_test

let test_get_key_count () = 
  let real_test client = 
    client # get_key_count () >>= fun result ->
    let msg = "Get key count should be zero but got " ^ (Int64.to_string result) in
    OUnit.assert_equal ~msg result 0L;
    let rec do_set = function 
    | 0 -> Lwt.return ()
    | i -> 
       let str = Printf.sprintf "%d" i in
       client # set str str >>= fun () ->
       do_set (i-1)
    in
    do_set 100 >>= fun () ->
    client # get_key_count () >>= fun result ->
    let msg = "Get key count should be 100 but got " ^ (Int64.to_string result) in
    OUnit.assert_equal ~msg result 100L;
    Lwt.return ()
  in 
  __client_server_wrapper__ _CLUSTER real_test


let test_and_set_to_none () =
  let real_test client =
    let key = "test_and_set_key" in
    let wanted_s = "value" in
    let wanted = Some wanted_s in
    let expected = None in
    client # test_and_set key expected wanted >>= fun result ->
    OUnit.assert_equal ~printer:string_option2s ~msg:"assert1" result wanted;
    client # test_and_set key wanted None >>= fun result2 ->
    OUnit.assert_equal ~printer:string_option2s ~msg:"assert2" result2 None;
    Lwt.return ()
  in __client_server_wrapper__ _CLUSTER real_test
  
let suite = "remote_client" >::: [
  "ping"      >:: test_ping;
  "wrong_cluster" >:: test_wrong_cluster;
  "set"        >:: test_set_get;
  "assert_exists" >:: test_assert_exists;
  "delete"     >:: test_delete;
  "range"      >:: test_range;
  "prefix_keys"  >:: test_prefix_keys;
  "test_and_set_to_none"  >:: test_and_set_to_none;
  "sequence"   >:: test_sequence;
  "user_function" >:: test_user_function;
  "get_key_count" >:: test_get_key_count;
  "confirm"    >:: test_confirm;
  "reverse_range" >:: test_reverse_range;
]
