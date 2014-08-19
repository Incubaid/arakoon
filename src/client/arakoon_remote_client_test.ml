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

open OUnit
open Lwt
open Log_extra
open Node_cfg

let section = Logger.Section.main

let should_fail f rc error_msg success_msg =
   Lwt.catch
     (fun ()  ->
       f () >>= fun () ->
       Logger.debug_ "should fail...doesn't")
    (function
        | Arakoon_exc.Exception(rc', _) when rc' = rc ->
           Logger.debug_ success_msg
        | exn ->
           Logger.debug_ ~exn error_msg >>= fun () ->
           Lwt.fail (Failure error_msg))

let cucu s =
  Lwt_io.eprintlf "cucu: %s" s >>= fun () ->
  Lwt_io.flush Lwt_io.stderr

let _CLUSTER = "sweety"

type real_test = Arakoon_client.client -> unit Lwt.t

let __client_server_wrapper__ (real_test:real_test) =
  let stop = ref false in
  let lease_period = 2 in
  let make_config () = Node_cfg.make_test_config
                         ~cluster_id:_CLUSTER
                         ~node_name:(Printf.sprintf "sweety_%i")
                         1 (Master_type.Elected) lease_period
  in
  let cluster_cfg = make_config () in
  let t0 = Node_main.test_t make_config "sweety_0" ~stop >>= fun _ -> Lwt.return () in

  let client_t () =
    let sp = float(lease_period) *. 0.5 in
    Lwt_unix.sleep sp >>= fun () -> (* let the cluster reach stability *)
    Logger.info_ "cluster should have reached stability" >>= fun () ->
    Client_main.find_master ~tls:None cluster_cfg >>= fun master_name ->
    Logger.info_f_ "master=%S" master_name >>= fun () ->
    let master_cfg =
      List.hd
        (List.filter (fun cfg -> cfg.Node_cfg.node_name = master_name) cluster_cfg.Node_cfg.cfgs)
    in
    Client_main.with_client
      ~tls:None
      master_cfg
      cluster_cfg.Node_cfg.cluster_id
      real_test
  in
  let main () =
    Lwt.pick [(t0 >>= fun () -> Lwt.return false);
              (client_t () >>= fun () -> Lwt.return true);] >>= fun succeeded ->
    stop := true;
    OUnit.assert_bool "client thread should finish before server" succeeded;
    Lwt.return ()
  in
  Lwt_main.run (main());;

let test_ping () =
  let real_test client =
    client # ping "test_ping" _CLUSTER >>= fun server_version ->
    OUnit.assert_equal
      server_version
      (Printf.sprintf
         "Arakoon %i.%i.%i"
         Arakoon_version.major Arakoon_version.minor Arakoon_version.patch);
    Lwt.return ()
  in __client_server_wrapper__ real_test

let test_wrong_cluster () =
  let wrong_cluster = "mindy" in
  let real_test client =
    Lwt.catch
      (fun () ->
         client # ping "boba fet" wrong_cluster >>= fun _result ->
         OUnit.assert_bool "we should not be able to connect to this cluster" false;
         Lwt.return ())
      (fun exn -> Logger.debug_f_ ~exn "ok, this cluster is not %s" wrong_cluster
        >>= fun () -> Lwt.return ())
    >>= fun () ->
    Lwt.return ()
  in __client_server_wrapper__ real_test

let test_set_get () =
  let real_test (client:Arakoon_client.client) =
    client # set "key" "value" >>= fun () ->
    client # get "key" >>= fun value ->
    OUnit.assert_equal value "value";
    Lwt.return ()
  in __client_server_wrapper__ real_test

let test_assert_exists () =
  let real_test (client:Arakoon_client.client) =
    client # set "key" "value" >>= fun () ->
    client # aSSert_exists "key" >>= fun () ->
    Lwt.return ()
  in __client_server_wrapper__ real_test

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
  __client_server_wrapper__ real_test

let test_delete () =
  let real_test (client:Arakoon_client.client) =
    client # set "key" "value" >>= fun () ->
    client # delete "key" >>= fun () ->
    should_fail
      (fun () -> Lwt.map ignore (client # get "key"))
      Arakoon_exc.E_NOT_FOUND
      "get of non_existing key does not throw correct exception"
      "ok!"
    >>= fun () ->
    Logger.info_ "part-2" >>= fun () ->
    should_fail
      (fun () -> client # delete "key")
      Arakoon_exc.E_NOT_FOUND
      "delete of non_existing key"
      "ok!"

  in __client_server_wrapper__ real_test

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
  __client_server_wrapper__ real_test


let _test_user_function (client:Arakoon_client.client) =
  let uf _user_db = function
    | None -> None
    | Some s ->
       let r = ref "" in
       String.iter (fun c -> r := (String.make 1 c ^ !r)) s;
       Some !r
  in
  Registry.Registry.register "reverse" uf;

  client # user_function "reverse" (Some "echo") >>= fun ro ->
  Logger.debug_f_ "we got %s" (string_option2s ro) >>= fun () ->
  OUnit.assert_equal ro  (Some "ohce");

  should_fail
    (fun () -> Lwt.map ignore (client # user_function "does not exist" None))
    Arakoon_exc.E_BAD_INPUT
    "non existing user function should throw exception"
    "ok!" >>= fun () ->

  Registry.Registry.register "notfound" (fun _ _ -> raise Not_found);
  Registry.Registry.register "failure" (fun _ _ -> raise (Failure ""));

  should_fail
    (fun () -> Lwt.map ignore (client # user_function "notfound" None))
    Arakoon_exc.E_NOT_FOUND
    "should throw not found"
    "ok!" >>= fun () ->

  should_fail
    (fun () -> Lwt.map ignore (client # user_function "failure" None))
    Arakoon_exc.E_USERFUNCTION_FAILURE
    "should throw E_USERFUNCTION_FAILURE"
    "ok!" >>= fun () ->
  Lwt.return ()

let test_user_function () =
  __client_server_wrapper__ _test_user_function

let test_user_hook () =
  Registry.HookRegistry.register "t3k"
                                 (fun (ic,oc,_) _ _ ->
                                  Llio.input_string ic >>= fun arg ->
                                  Llio.output_string oc ("cucu " ^ arg));
  Registry.HookRegistry.register "rev_range"
                                 (fun (_,oc,_) user_db _ ->
                                  let _count, keys = user_db # with_cursor
                                                         (fun cur ->
                                                          Registry.Cursor_store.fold_rev_range
                                                            cur
                                                            (Some "t")
                                                            false
                                                            "a"
                                                            false
                                                            (-1)
                                                            (fun _ k _ acc ->
                                                             k :: acc)
                                                            []) in
                                  Llio.output_list Llio.output_key oc keys
                                 );
  __client_server_wrapper__ (fun client ->
                             client # user_hook "t3k" >>= fun (ic,oc) ->
                             Llio.output_string oc "cthulhu" >>= fun () ->
                             Llio.input_string ic >>= fun response ->
                             OUnit.assert_equal response "cucu cthulhu";

                             let keys = ["a"; "b"; "c"; "f"; "t"] in
                             Lwt_list.iter_p (fun k -> client # set k k) keys >>= fun () ->

                             client # user_hook "rev_range" >>= fun (ic,_) ->
                             Llio.input_list Llio.input_string ic >>= fun response ->
                             OUnit.assert_equal response ["f"; "c"; "b"];
                             Lwt.return ())

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
  client # rev_range_entries ~consistency:Arakoon_client.Consistent ~first:(Some "xey100") ~finc:true ~last:(Some "xey009") ~linc:true ~max:3 >>= fun xn ->
  Lwt_list.iter_s (fun (k, _) -> Logger.debug_f_ "key %s" k) xn >>= fun () ->
  let k,_ = List.hd xn in
  let () = OUnit.assert_bool "hd" (k  = "xey099") in
  Lwt.return ()

let test_range () = __client_server_wrapper__ _test_range

let test_reverse_range () = __client_server_wrapper__ _test_reverse_range

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
  __client_server_wrapper__ _prefix_keys_test

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
  __client_server_wrapper__ real_test


let test_and_set_to_none () =
  let real_test client =
    let key = "test_and_set_key" in
    let wanted_s = "value" in
    let wanted = Some wanted_s in
    let expected = None in
    client # test_and_set key expected wanted >>= fun result ->
    OUnit.assert_equal ~printer:string_option2s ~msg:"assert1" result None;
    client # test_and_set key wanted None >>= fun result2 ->
    OUnit.assert_equal ~printer:string_option2s ~msg:"assert2" result2 wanted;
    Lwt.return ()
  in __client_server_wrapper__ real_test

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
    "user_hook" >:: test_user_hook;
    "get_key_count" >:: test_get_key_count;
    "confirm"    >:: test_confirm;
    "reverse_range" >:: test_reverse_range;
  ]
