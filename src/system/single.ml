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
open Node_cfg.Node_cfg
open Arakoon_client
open Master_type

let section = Logger.Section.main

let should_fail = Arakoon_remote_client_test.should_fail

let _start tn =
  Logger.info_f_ "---------------------%s--------------------" tn

let all_same_master (_tn, cluster_cfg, all_t) =
  let scenario () =
    let q = float (cluster_cfg._lease_period) *. 1.5 in
    Lwt_unix.sleep q >>= fun () ->
    Logger.debug_ "start of scenario" >>= fun () ->
    let set_one client = client # set "key" "value" in
    Client_main.find_master ~tls:None cluster_cfg >>= fun master_name ->
    let master_cfg = List.hd (List.filter (fun cfg -> cfg.node_name = master_name)
                                cluster_cfg.cfgs)
    in
    Client_main.with_client ~tls:None master_cfg cluster_cfg.cluster_id set_one >>= fun () ->
    let masters = ref [] in
    let do_one cfg =
      let nn = node_name cfg in
      Logger.info_f_ "cfg:name=%s" nn  >>= fun () ->
      let f client =
        client # who_master () >>= function master ->
          masters := master :: !masters;
          Logger.info_f_ "Client:%s got: %s" nn (Log_extra.string_option2s master)
      in
      Client_main.with_client ~tls:None cfg cluster_cfg.cluster_id f
    in
    let cfgs = cluster_cfg.cfgs in
    Lwt_list.iter_s do_one cfgs >>= fun () ->
    assert_equal ~printer:string_of_int
      (List.length cfgs)
      (List.length !masters);
    let test = function
      | [] -> assert_failure "can't happen"
      | s :: rest ->
        begin
          List.iter
            (fun s' ->
               if s <> s' then assert_failure "different"
               else match s with | None -> assert_failure "None" | _ -> ()
            )
            rest
        end
    in
    Logger.debug_ "all_same_master:testing" >>= fun () ->
    let () = test !masters in
    Lwt.return ()
  in
  Lwt.pick [Lwt.join all_t;
            scenario () ]


let nothing_on_slave (_tn, cluster_cfg, all_t) =
  let cfgs = cluster_cfg.cfgs in
  let find_slaves cfgs =
    Client_main.find_master ~tls:None cluster_cfg >>= fun m ->
    let slave_cfgs = List.filter (fun cfg -> cfg.node_name <> m) cfgs in
    Lwt.return slave_cfgs
  in
  let named_fail name f =
    should_fail
      f
      Arakoon_exc.E_NOT_MASTER
      (name ^ " should not succeed on slave")
      (name ^ " failed on slave, which is intended")
  in
  let set_on_slave client =
    named_fail "set" (fun () -> client # set "key" "value")
  in
  let delete_on_slave client =
    named_fail "delete" (fun () -> client # delete "key")
  in
  let test_and_set_on_slave client =
    named_fail "test_and_set"
      (fun () ->
         let wanted = Some "value!" in
         client # test_and_set "key" None wanted >>= fun _ ->
         Lwt.return ()
      )
  in

  let test_slave cluster_id cfg =
    Logger.info_f_ "slave=%s" cfg.node_name  >>= fun () ->
    let f with_client =
      with_client set_on_slave >>= fun () ->
      with_client delete_on_slave >>= fun () ->
      with_client test_and_set_on_slave
    in
    f (fun client_action -> Client_main.with_client ~tls:None cfg cluster_id client_action)
  in
  let test_slaves ccfg =
    find_slaves cfgs >>= fun slave_cfgs ->
    let rec loop = function
      | [] -> Lwt.return ()
      | cfg :: rest -> test_slave ccfg.cluster_id cfg >>= fun () ->
        loop rest
    in loop slave_cfgs
  in
  Lwt.pick [Lwt.join all_t;
            Lwt_unix.sleep 5.0 >>= fun () -> test_slaves cluster_cfg]

let dirty_on_slave (_tn, cluster_cfg,_) =
  Lwt_unix.sleep (float (cluster_cfg._lease_period)) >>= fun () ->
  Logger.debug_ "dirty_on_slave" >>= fun () ->
  let cfgs = cluster_cfg.cfgs in
  Client_main.find_master ~tls:None cluster_cfg >>= fun master_name ->
  let master_cfg = List.hd (List.filter (fun cfg -> cfg.node_name = master_name)
                              cluster_cfg.cfgs)
  in
  Client_main.with_client ~tls:None master_cfg cluster_cfg.cluster_id
    (fun client -> client # set "xxx" "xxx")
  >>= fun () ->

  let find_slaves cfgs =
    let slave_cfgs = List.filter (fun cfg -> cfg.node_name <> master_name) cfgs in
    Lwt.return slave_cfgs
  in
  let do_slave cluster_id cfg =
    let dirty_get (client:Arakoon_client.client) =
      Logger.debug_ "dirty_get" >>= fun () ->
      should_fail
        (fun () ->
         client # get ~consistency:No_guarantees "xxx" >>= fun v ->
         Logger.debug_f_ "dirty_get:result = %s" v)
        Arakoon_exc.E_NOT_FOUND
        "dirty get should fail with not found"
        "dirty get failed with not found as intended"
    in
    Client_main.with_client ~tls:None cfg cluster_id dirty_get
  in
  let do_slaves ccfg =
    find_slaves cfgs >>= fun slave_cfgs ->
    let rec loop = function
      | [] -> Lwt.return ()
      | cfg::rest -> do_slave ccfg.cluster_id cfg >>= fun () ->
        loop rest
    in
    loop slave_cfgs
  in
  do_slaves cluster_cfg

type client = Arakoon_client.client

let _get_after_set (client:client) =
  client # set "set" "some_value" >>= fun () ->
  client # get "set" >>= fun value ->
  if value <> "some_value"
  then Llio.lwt_failfmt "\"%S\" <> \"some_value\"" value
  else Lwt.return ()

let _exists (client:client) =
  client # set "Geronimo" "Stilton" >>= fun () ->
  client # exists "Geronimo" >>= fun yes ->
  client # exists "Mickey" >>= fun no ->
  begin
    if yes
    then Logger.info_f_ "exists yields true, which was expected"
    else Llio.lwt_failfmt "Geronimo should be in there"
  end >>= fun () ->
  begin
    if no then
      Llio.lwt_failfmt "Mickey should not be in there"
    else
      Logger.info_f_ " Mickey's not in there, as expected"
  end

let _delete_after_set (client:client) =
  let key = "_delete_after_set" in
  client # set key "xxx" >>= fun () ->
  client # delete key    >>= fun () ->
  should_fail
    (fun () ->
       client # get "delete"  >>= fun v ->
       Logger.info_f_ "delete_after_set get yields value=%S" v >>= fun () ->
       Lwt.return ()
    )
    Arakoon_exc.E_NOT_FOUND
    "get after delete yields, which is A PROBLEM!"
    "get after delete fails, which was intended"

let _test_and_set_1 (client:client) =
  Logger.info_f_ "_test_and_set_1" >>= fun () ->
  let wanted_s = "value!" in
  let wanted = Some wanted_s in
  let key = "_test_and_set_1__" in
  client # test_and_set key None wanted >>= fun result ->
  begin
    match result with
      | Some v -> Llio.lwt_failfmt "result should be None, got %S" v
      | None -> Logger.info_f_ "result is None, as expected"
  end >>= fun () ->
  client # get key >>= fun result ->
  OUnit.assert_equal result wanted_s;
  client # delete key >>= fun () ->
  Lwt.return ()


let _test_and_set_2 (client: client) =
  Logger.info_f_ "_test_and_set_2" >>= fun () ->
  let wanted = Some "wrong!" in
  client # test_and_set "test_and_set" (Some "x") wanted
  >>= fun result ->
  begin
    match result with
      | None -> Logger.info_f_ "value is None, which is intended"
      | Some x -> Llio.lwt_failfmt "value='%S', which is unexpected" x
  end >>= fun () ->
  Lwt.return ()

let _test_and_set_3 (client: client) =
  Logger.info_f_ "_test_and_set_3" >>= fun () ->
  let key = "_test_and_set_3__" in
  let value = "bla bla" in
  client # set key value >>= fun () ->
  client # test_and_set key (Some value) None >>= fun result ->
  begin
    match result with
      | Some v' when v' = value -> Logger.info_f_ "value=%S, which is what we expected" value
      | _ -> Llio.lwt_failfmt "expected %s but got %s" (Log_extra.string_option2s (Some value)) (Log_extra.string_option2s result)
  end >>= fun () ->
  client # exists key >>= fun b ->
  if b then
    Llio.lwt_failfmt "we should have deleted this"
  else Lwt.return ()

let _replace (client:client) =
  Logger.info_f_ "_replace" >>= fun () ->
  let v = "Wanted" in
  let key = "replace" in
  client # replace key (Some v) >>= fun vo ->
  begin
    match vo with
    | None -> Logger.info_f_ "part 1: value is None, as expected"
    | Some x -> Llio.lwt_failfmt "value='%S', which is wrong" x
  end >>= fun () ->
  let v2 = "Wanted2" in
  client # replace key (Some v2) >>= fun vo2 ->
  begin
    match vo2 with
    | None -> Llio.lwt_failfmt "value is None, which is wrong"
    | Some x ->
       Logger.info_f_ "part 2: got: %s" x >>= fun () ->
       OUnit.assert_equal x v ~printer:(fun s -> s); Lwt.return ()
  end >>= fun () ->
  client # replace key None >>= fun vo2 ->
  Logger.info_f_ "part 3: got %s" (Log_extra.string_option2s vo2) >>= fun () ->
  OUnit.assert_equal vo2 (Some v2);

  client # exists key >>= fun exists ->
  OUnit.assert_bool "value should not exist here" (not exists);
  Lwt.return ()


let _assert1 (client: client) =
  Logger.info_ "_assert1" >>= fun () ->
  client # set "my_value" "my_value" >>= fun () ->
  client # aSSert "my_value" (Some "my_value") >>= fun () ->
  Lwt.return ()

let _assert2 (client: client) =
  Logger.info_ "_assert2" >>= fun () ->
  client # set "x" "x" >>= fun () ->
  should_fail
    (fun () -> client # aSSert "x" (Some "y"))
    Arakoon_exc.E_ASSERTION_FAILED
    "PROBLEM:_assert2: yielded unit"
    "_assert2: ok, this aSSert should indeed fail"

let _assert3 (client:client) =
  Logger.info_ "_assert3" >>= fun () ->
  let k = "_assert3" in
  client # set k k >>= fun () ->
  Logger.info_ "_assert3: value set" >>= fun () ->
  let updates = [
    Arakoon_client.Assert(k,Some k);
    Arakoon_client.Set(k, "REALLY")
  ]
  in
  client # sequence updates >>= fun () ->
  client # get k >>= fun v2 ->
  OUnit.assert_equal v2 "REALLY";
  should_fail
    (fun () ->
       client # sequence updates >>= fun () ->
       let u2 = [
         Arakoon_client.Assert(k,Some k);
         Arakoon_client.Set(k,"NO WAY")
       ]
       in
       client # sequence u2)
    Arakoon_exc.E_ASSERTION_FAILED
    "sequence should fail with E_ASSERTION_FAILED"
    "sequence should fail with E_ASSERTION_FAILED"
  >>= fun () ->
  client # get k >>= fun  v2 ->
  OUnit.assert_equal v2 "REALLY";
  Lwt.return ()

let _assert_exists1 (client: client) =
  client # set "my_value_exists" "my_value_exists" >>= fun () ->
  client # aSSert_exists "my_value_exists" >>= fun () ->
  Lwt.return ()

let _assert_exists2 (client: client) =
  client # set "x_exists" "x_exists" >>= fun () ->
  should_fail
    (fun () -> client # aSSert_exists "x_no_exists")
    Arakoon_exc.E_ASSERTION_FAILED
    "PROBLEM:_assert_exists2: yielded unit"
    "_assert_exists2: ok, this aSSert should indeed fail"

let _assert_exists3 (client:client) =
  let k = "_assert_exists3" in
  client # set k k >>= fun () ->
  Logger.info_ "_assert_exists3: value set" >>= fun () ->
  let updates = [
    Arakoon_client.Assert_exists(k);
    Arakoon_client.Set(k, "REALLY")
  ]
  in
  client # sequence updates >>= fun () ->
  client # get k >>= fun v2 ->
  OUnit.assert_equal v2 "REALLY";
  client # sequence updates >>= fun () ->
  let u2 = [
    Arakoon_client.Assert_exists(k);
    Arakoon_client.Set(k,"YES WAY")
  ]
  in
  client # sequence u2 >>= fun () ->
  client # get k >>= fun  v3 ->
  OUnit.assert_equal v3 "YES WAY";
  Lwt.return ()

let _range_1 (client: client) =
  let rec fill i =
    if i = 100
    then Lwt.return ()
    else
      let key = "range_" ^ (string_of_int i)
      and value = (string_of_int i) in
      client # set key value >>= fun () -> fill (i+1)
  in fill 0 >>= fun () ->
  client # range (Some "range_1") true (Some "rs") true 10 >>= fun keys ->
  let size = List.length keys in
  Logger.info_f_ "size = %i" size >>= fun () ->
  if size <> 10
  then Llio.lwt_failfmt "_range_1: size should be 10 and is %i" size
  else Lwt.return ()

let _range_entries_1 (client: client) =
  Logger.info_f_ "_range_entries_1" >>= fun () ->
  let rec fill i =
    if i = 100
    then Lwt.return ()
    else
      let key = "range_entries_" ^ (string_of_int i)
      and value = (string_of_int i) in
      client # set key value >>= fun () -> fill (i+1)
  in fill 0 >>= fun () ->
  client # range_entries ~consistency:Arakoon_client.Consistent ~first:(Some "range_entries") ~finc:true ~last:(Some "rs") ~linc:true ~max:10 >>= fun entries ->
  let size = List.length entries in
  Logger.info_f_ "size = %i" size >>= fun () ->
  if size <> 10
  then Llio.lwt_failfmt "_range_entries_1: size should be 10 and is %i" size
  else Lwt.return ()

let _detailed_range client =
  Logger.info_f_ "_detailed_range" >>= fun () ->
  Arakoon_remote_client_test._test_range client

let _prefix_keys (client:client) =
  Logger.info_f_ "_prefix_keys" >>= fun () ->
  Arakoon_remote_client_test._prefix_keys_test client

(* TODO: nodestream test
   let _list_entries (client:Nodestream.nodestream) =
   Logger.info_f_ "_list_entries" >>= fun () ->
   let filename = "/tmp/_list_entries.tlog" in
   Lwt_io.with_file filename ~mode:Lwt_io.output
    (fun oc ->
      let f (i,update) = Lwt.return () in
      client # iterate Sn.start f)
*)
let _sequence (client: client) =
  Logger.info_f_ "_sequence" >>= fun () ->
  client # set "XXX0" "YYY0" >>= fun () ->
  let updates = [Arakoon_client.Set("XXX1","YYY1");
                 Arakoon_client.Set("XXX2","YYY2");
                 Arakoon_client.Set("XXX3","YYY3");
                 Arakoon_client.Delete "XXX0";
                ]
  in
  client # sequence updates >>= fun () ->
  client # get "XXX1" >>= fun v1 ->
  OUnit.assert_equal v1 "YYY1";
  client # get "XXX2" >>= fun v2 ->
  OUnit.assert_equal v2 "YYY2";
  client # get "XXX3">>= fun v3 ->
  OUnit.assert_equal v3 "YYY3";
  client # exists "XXX0" >>= fun exists ->
  OUnit.assert_bool "XXX0 should not be there" (not exists);
  Lwt.return ()

let _sequence2 (client: client) =
  Logger.info_f_ "_sequence" >>= fun () ->
  let k1 = "I_DO_NOT_EXIST" in
  let k2 = "I_SHOULD_NOT_EXIST" in
  let updates = [
    Arakoon_client.Delete(k1);
    Arakoon_client.Set(k2, "REALLY")
  ]
  in
  should_fail
    (fun () -> client # sequence updates)
    Arakoon_exc.E_NOT_FOUND
    "_sequence2:failing delete in sequence does not produce exception"
    "_sequence2:produced exception, which is intended"
  >>= fun ()->
  Logger.debug_ "_sequence2: part 2 of scenario" >>= fun () ->
  should_fail
    (fun () -> client # get k2 >>= fun _ -> Lwt.return ())
    Arakoon_exc.E_NOT_FOUND
    "PROBLEM:_sequence2: get yielded a value"
    "_sequence2: ok, this get should indeed fail"
  >>= fun () -> Lwt.return ()

let _sequence3 (client: client) =
  Logger.info_f_ "_sequence3" >>= fun () ->
  let k1 = "sequence3:key1"
  and k2 = "sequence3:key2"
  in
  let changes = [Arakoon_client.Set (k1,k1 ^ ":value");
                 Arakoon_client.Delete k2;] in
  should_fail
    (fun () -> client # sequence changes)
    Arakoon_exc.E_NOT_FOUND
    "PROBLEM: _sequence3: change should fail (exception in change)"
    "sequence3 changes indeed failed"
  >>= fun () ->
  should_fail
    (fun () -> client # get k1 >>= fun v1 -> Logger.info_f_ "value=:%s" v1)
    Arakoon_exc.E_NOT_FOUND
    "PROBLEM: changes should be all or nothing"
    "ok: all-or-noting changes"
  >>= fun () -> Logger.info_f_ "sequence3.ok"

let _progress_possible (client:client) =
  Logger.info_f_ "_progress_possible" >>= fun () ->
  client # expect_progress_possible () >>= fun b ->
  OUnit.assert_equal ~msg:"we should have the possibility of progress here" b true;
  Lwt.return ()

let _multi_get (client: client) =
  let key1 = "_multi_get:key1"
  and key2 = "_multi_get:key2"
  in
  client # set key1 key1 >>= fun () ->
  client # set key2 key2 >>= fun () ->
  client # multi_get [key1;key2] >>= fun values ->
  begin
    match values with
      | [v1;v2] ->
        Logger.debug_f_ "v1=%S;v2=%S" v1 v2
        >>= fun () ->
        OUnit.assert_equal v1 key1;
        OUnit.assert_equal v2 key2;
        Lwt.return ()
      | _ -> Lwt.fail (Failure "2 values expected")
  end >>= fun () ->
  should_fail
    (fun () ->
       client # multi_get ["I_DO_NOT_EXIST";key2]
       >>= fun _values ->
       Lwt.return ())
    Arakoon_exc.E_NOT_FOUND
    "should fail with E_NOT_FOUND"
    "should fail with E_NOT_FOUND"

let _multi_get_option (client:client) =
  let k1 = "_multi_get_option:key1"
  and k2 = "_multi_get_option:key2"
  in
  client # set k1 k1 >>= fun () ->
  client # set k2 k2 >>= fun () ->
  client # multi_get_option [k1;k2;"?"] >>= fun vos ->
  match vos with
    | [Some v1;Some v2;None] ->
      let id s = s in
      OUnit.assert_equal ~printer:id v1 k1;
      OUnit.assert_equal ~printer:id v2 k2;
      Lwt.return ()
    | _ ->
      Lwt_list.iter_s (fun so -> Lwt_io.printlf "vos.... %S" (Log_extra.string_option2s so)) vos >>= fun () ->
      Lwt.fail (Failure "bad order or arity")


let _with_master ((_tn:string), cluster_cfg, _) f =
  let sp = float(cluster_cfg._lease_period) *. 0.5 in
  Lwt_unix.sleep sp >>= fun () -> (* let the cluster reach stability *)
  Logger.info_ "cluster should have reached stability" >>= fun () ->
  Client_main.find_master ~tls:None cluster_cfg >>= fun master_name ->
  Logger.info_f_ "master=%S" master_name >>= fun () ->
  let master_cfg =
    List.hd
      (List.filter (fun cfg -> cfg.node_name = master_name) cluster_cfg.cfgs)
  in
  Client_main.with_client ~tls:None master_cfg cluster_cfg.cluster_id f


let trivial_master tpl =
  let f (client:client) =
    _get_after_set client >>= fun () ->
    _delete_after_set client >>= fun () ->
    _exists client >>= fun () ->
    _test_and_set_1 client >>= fun () ->
    _test_and_set_2 client >>= fun () ->
    _test_and_set_3 client
  in
  _with_master tpl f


let trivial_master2 tpl =
  let f client =
    _test_and_set_3 client >>= fun () ->
    _range_1 client >>= fun () ->
    _range_entries_1 client >>= fun () ->
    _prefix_keys client >>= fun () ->
    _detailed_range client
  in
  _with_master tpl f


let trivial_master3 tpl =
  let f client =
    _sequence client >>= fun () ->
    _sequence2 client >>= fun () ->
    _sequence3 client
  in
  _with_master tpl f

let replace tpl = _with_master tpl _replace

let trivial_master4 tpl = _with_master tpl _multi_get

let trivial_master5 tpl = _with_master tpl _progress_possible

let trivial_master6 tpl = _with_master tpl _multi_get_option

let assert1 tpl = _with_master tpl _assert1

let assert2 tpl = _with_master tpl _assert2

let assert3 tpl =
  _start "assert3" >>= fun () ->
  _with_master tpl _assert3

let assert_exists1 tpl = _with_master tpl _assert_exists1

let assert_exists2 tpl = _with_master tpl _assert_exists2

let assert_exists3 tpl =
  _start "assert_exists3" >>= fun () ->
  _with_master tpl _assert_exists3

let _node_name tn n = Printf.sprintf "%s_%i" tn n

let stop = ref (ref false)

let setup make_master tn base () =
  _start tn >>= fun () ->
  let lease_period = 10 in
  let cluster_id = Printf.sprintf "%s_%i" tn base in
  let master = make_master tn 0 in
  let make_config () = Node_cfg.Node_cfg.make_test_config
                         ~base ~cluster_id
                         ~node_name:(_node_name tn)
                         3 master lease_period
  in
  let stop = !stop in
  let t0 = Node_main.test_t ~stop make_config (_node_name tn 0) >>= fun _ -> Lwt.return () in
  let t1 = Node_main.test_t ~stop make_config (_node_name tn 1) >>= fun _ -> Lwt.return () in
  let t2 = Node_main.test_t ~stop make_config (_node_name tn 2) >>= fun _ -> Lwt.return () in
  let all_t = [t0;t1;t2] in
  Lwt.return (tn, make_config (), all_t)

let teardown (tn, _, all_t) =
  !stop := true;
  stop := ref false;
  Logger.info_f_ "++++++++++++++++++++ %s +++++++++++++++++++" tn >>= fun () ->
  Lwt.join all_t

let make_suite base name w =
  let make_el tn b f = tn >:: w tn b f in
  name >:::
    [
      make_el "all_same_master" base all_same_master;
      make_el "nothing_on_slave" (base + 100) nothing_on_slave;
      make_el "dirty_on_slave"   (base + 200) dirty_on_slave;
      make_el "trivial_master"   (base + 300) trivial_master;
      make_el "trivial_master2"  (base + 400) trivial_master2;
      make_el "trivial_master3"  (base + 500) trivial_master3;
      make_el "trivial_master4"  (base + 600) trivial_master4;
      make_el "trivial_master5"  (base + 700) trivial_master5;
      make_el "assert1"          (base + 800) assert1;
      make_el "assert2"          (base + 900) assert2;
      make_el "assert3"          (base + 1000) assert3;
      make_el "assert_exists1"   (base + 1100) assert_exists1;
      make_el "assert_exists2"   (base + 1200) assert_exists2;
      make_el "assert_exists3"   (base + 1300) assert_exists3;
      make_el "trivial_master6"  (base + 1400) trivial_master6;
      make_el "replace"          (base + 1500) replace
    ]

let force_master =
  let make_master tn n = Forced (_node_name tn  n) in
  let w tn base f = Extra.lwt_bracket (setup make_master tn base) f teardown in
  make_suite 4000 "force_master" w


let elect_master =
  let make_master _tn _ = Elected in
  let w tn base f = Extra.lwt_bracket (setup make_master tn base) f teardown in
  make_suite 6000 "elect_master" w
