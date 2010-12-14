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
open Lwt_log
open Log_extra
open Arakoon_remote_client
open Test_backend
open Update

let cucu s =
  Lwt_io.eprintlf "cucu: %s" s >>= fun () ->
  Lwt_io.flush Lwt_io.stderr


let __client_server_wrapper__ real_test =
  let port = 7777 in
  let conversation connection  =
    cucu "started conversation" >>= fun () ->
    let client = new remote_client connection in
      real_test client >>= fun () -> Lwt.return ()
  in
  let sleep, notifier = wait () in
  let setup_callback () =
    info "callback" >>= fun () ->
    Lwt.wakeup notifier ();
    Lwt.return ()
  in
  let backend = new test_backend "Zen" in
  let server = Server.make_server_thread ~setup_callback "127.0.0.1" port
    (Client_protocol.protocol backend) in

  let client_t () =
    let address = Unix.ADDR_INET (Unix.inet_addr_loopback, port) in
    Lwt_io.open_connection address >>= function (ic,oc) ->
    conversation (ic,oc) >>= fun () ->
    debug "end_of_senario" >>= fun () ->
    Lwt_io.close ic >>= fun () ->
    Lwt_io.close oc >>= fun () ->
    Lwt.return ()
  in

    Lwt_main.run (Lwt.pick [client_t ();
          server ();])

let test_hello () =
  let real_test client =
    client # hello "test_hello" >>= fun server_version ->
    OUnit.assert_equal server_version "test_backend.0.0.0";
    Lwt.return ()
  in __client_server_wrapper__ real_test


let test_set_get () =
  let real_test client =
    client # set "key" "value" >>= fun () ->
    client # get "key" >>= fun value ->
    OUnit.assert_equal value "value";
    Lwt.return ()
  in __client_server_wrapper__ real_test

let test_delete () =
  let real_test client =
    client # set "key" "value" >>= fun () ->
    client # delete "key" >>= fun () ->
    Lwt.catch
      (fun () -> 
	client # get "key" >>= fun value ->
	Lwt.return ())
      (function
	| Arakoon_exc.Exception (Arakoon_exc.E_NOT_FOUND,_) -> 
	  Lwt_io.printlf "ok!"
	| exn ->
	  Lwt_log.fatal ~exn "wrong exception">>= fun () ->
	  OUnit.assert_failure 
	    "get of non_existing key does not throw correct exception"
      ) 
    >>= fun () ->
    Lwt_log.info "part-2" >>= fun () ->
    Lwt.catch
      (fun () ->
	client # delete "key" >>= fun () -> 
	Lwt_log.info "should not get here"
      )
      (function
	| Arakoon_exc.Exception (Arakoon_exc.E_NOT_FOUND, _) -> 
	  Lwt.return ()
	| exn -> Lwt_log.fatal ~exn "should not be" >>= fun () ->
	  OUnit.assert_failure "XXX"
      )
      
  in __client_server_wrapper__ real_test

let test_sequence () =
  let real_test client = 
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
  __client_server_wrapper__ real_test


let _test_range client =
  let clear () =
    client # range None true None true 1000 >>= fun xn ->
    Lwt_list.iter_s
      (fun x -> client # delete x)
      xn
  in
  let rec fill n =
    let cat s i = Printf.sprintf "%s%03d" s i in
    if n = 100 then Lwt.return ()
    else
      client # set (cat "xey" n) (cat "value" n) >>= fun () ->
    fill (n+1)
  in
  clear () >>= fun () ->
  fill 0 >>= fun () ->
  client # range (Some "xey") true (Some "xey009") true 3 >>= fun xn ->
  let length = List.length xn in
  Lwt_log.debug_f "received a list of size %i" length >>= fun () ->
  let msg = Printf.sprintf "expecting length: 3, actual: %d" length in
  let () = OUnit.assert_equal ~msg 3 length in
  Lwt_list.iter_s (fun x -> Lwt_log.debug_f "key %s" x) xn >>= fun () ->
  let () = OUnit.assert_bool "0" (List.mem "xey000" xn) in
  let () = OUnit.assert_bool "1" (List.mem "xey001" xn) in
  let () = OUnit.assert_bool "2" (List.mem "xey002" xn) in
  client # range None true (Some "xey009") true 11 >>= fun xn ->
  (* let xn = List.filter (fun x -> x.[0] <> '@' && x.[1] <> '@') xn in *)
  let length = List.length xn in
  Lwt_list.iter_s (fun x -> Lwt_log.debug_f "key %s" x) xn >>= fun () ->
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
  Lwt_list.iter_s (fun x -> Lwt_log.debug_f "key %s" x) xn >>= fun () ->
  let msg = Printf.sprintf "expecting length: 1, actual: %d" length in
  let () = OUnit.assert_equal ~msg 1 length in
  let () = OUnit.assert_bool "x0" (List.mem "xey000" xn) in
  Lwt.return ()

let test_range () =
  __client_server_wrapper__ _test_range

let _prefix_keys_test client =
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
  Lwt_log.debug_f "received a list of size %i" length >>= fun () ->
  let () = OUnit.assert_equal 100 length in
  let () = OUnit.assert_bool "0" (List.mem "key0" list) in
  let () = OUnit.assert_bool "99" (List.mem "key99" list) in
  let () = OUnit.assert_bool "foo" (not (List.mem "foo" list)) in
  let () = OUnit.assert_bool "foo2" (not (List.mem "foo2" list)) in
  Lwt.return ()

let test_prefix_keys () =
  __client_server_wrapper__ _prefix_keys_test

let test_and_set_to_none () =
  let real_test client =
    let key = "test_and_set_key" in
    let wanted_s = "value" in
    let wanted = Some wanted_s in
    let expected = None in
    client # test_and_set key expected wanted >>= fun result ->
    OUnit.assert_equal ~printer:string_option_to_string ~msg:"assert1" result wanted;
    client # test_and_set key wanted None >>= fun result2 ->
    OUnit.assert_equal ~printer:string_option_to_string ~msg:"assert2" result2 None;
    Lwt.return ()
  in __client_server_wrapper__ real_test
  
let suite = "remote_client" >::: [
  "hello"      >:: test_hello;
  "set"        >:: test_set_get;
  "delete"     >:: test_delete;
  "range"      >:: test_range;
  "prefix_keys"  >:: test_prefix_keys;
  "test_and_set_to_none"  >:: test_and_set_to_none;
  "sequence"   >:: test_sequence;
]
