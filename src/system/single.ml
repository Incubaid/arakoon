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
open Node_cfg.Node_cfg
open Arakoon_remote_client
open Network
open Update
open Master_type

let should_fail x error_msg success_msg =
  Lwt.catch 
    (fun ()  -> 
      x () >>= fun () -> 
      Lwt_log.debug "should fail...doesn't" >>= fun () ->
      Lwt.return true) 
    (fun exn -> Lwt_log.debug ~exn success_msg >>= fun () -> Lwt.return false)
  >>= fun bad -> 
  if bad then Lwt.fail (Failure error_msg)
  else Lwt.return ()

let _start tn = 
  let d = 5.0 in
  Lwt_log.info_f "START of %s (& sleep %f" tn d >>= fun () ->
  Lwt_unix.sleep d

let all_same_master (cluster_cfg, all_t) =
  _start "all_same_master" >>= fun () ->
  let scenario () = 
    Lwt_log.debug "start of scenario" >>= fun () ->
    let set_one client = client # set "key" "value" in
    Client_main.find_master cluster_cfg >>= fun master_name ->
    let master_cfg = List.hd (List.filter (fun cfg -> cfg.node_name = master_name) 
				cluster_cfg.cfgs) 
    in
    Client_main.with_client master_cfg cluster_cfg.cluster_id set_one >>= fun () ->
    let masters = ref [] in
    let do_one cfg =
      Lwt_log.info_f "cfg:name=%s" (node_name cfg)  >>= fun () ->
      let f client =
	client # who_master () >>= function master ->
	  masters := master :: !masters;
	  Lwt.return ()
      in
      Client_main.with_client cfg cluster_cfg.cluster_id f
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
    Lwt_log.debug "all_same_master:testing" >>= fun () ->
    let () = test !masters in
    Lwt.return ()
  in
  Lwt.pick [Lwt.join all_t;
	    scenario () ]
  

let nothing_on_slave (cluster_cfg, _) =
  let cfgs = cluster_cfg.cfgs in
  let find_slaves cfgs =
    Client_main.find_master cluster_cfg >>= fun m ->
      let slave_cfgs = List.filter (fun cfg -> cfg.node_name <> m) cfgs in
	Lwt.return slave_cfgs
  in
  let named_fail name f =
    should_fail f
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
    Lwt_log.info_f "slave=%s" cfg.node_name  >>= fun () ->
      let f (client:Arakoon_client.client) =
	set_on_slave client >>= fun () ->
        delete_on_slave client >>= fun () ->
	test_and_set_on_slave client 
      in
	Client_main.with_client cfg cluster_id f
  in
  let test_slaves ccfg =
    find_slaves cfgs >>= fun slave_cfgs ->
      let rec loop = function
	| [] -> Lwt.return ()
	| cfg :: rest -> test_slave ccfg.cluster_id cfg >>= fun () ->
	    loop rest
      in loop slave_cfgs
  in
  test_slaves cluster_cfg

let dirty_on_slave (cluster_cfg,_) = 
  Lwt_unix.sleep (float (cluster_cfg._lease_period)) >>= fun () ->
  Lwt_log.debug "dirty_on_slave" >>= fun () ->
  let cfgs = cluster_cfg.cfgs in
  Client_main.find_master cluster_cfg >>= fun master_name ->
  let master_cfg = List.hd (List.filter (fun cfg -> cfg.node_name = master_name) 
			      cluster_cfg.cfgs)
  in
  Client_main.with_client master_cfg cluster_cfg.cluster_id
    (fun client -> client # set "xxx" "xxx")
  >>= fun () ->

  let find_slaves cfgs =
    let slave_cfgs = List.filter (fun cfg -> cfg.node_name <> master_name) cfgs in
    Lwt.return slave_cfgs
  in
  let do_slave cluster_id cfg = 
    let dirty_get (client:Arakoon_client.client) = 
      Lwt_log.debug "dirty_get" >>= fun () ->
      Lwt.catch
	(fun () -> client # get ~allow_dirty:true "xxx" >>= fun v ->
	  Lwt_log.debug_f "dirty_get:result = %s" v 
	)
	(function 
	  | Arakoon_exc.Exception(Arakoon_exc.E_NOT_FOUND,"xxx") -> 
	    Lwt_log.debug "dirty_get yielded a not_found" 
	  | e -> Lwt.fail e)
    in
    Client_main.with_client cfg cluster_id dirty_get
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
    then Lwt_log.info_f "exists yields true, which was expected"
    else Llio.lwt_failfmt "Geronimo should be in there"
  end >>= fun () ->
  begin
    if no then
      Llio.lwt_failfmt "Mickey should not be in there"
    else
      Lwt_log.info_f" Mickey's not in there, as expected"
  end

let _delete_after_set (client:client) =
  let key = "_delete_after_set" in
  client # set key "xxx" >>= fun () ->
  client # delete key    >>= fun () ->
  should_fail
    (fun () ->
      client # get "delete"  >>= fun v ->
      Lwt_log.info_f "delete_after_set get yields value=%S" v >>= fun () ->
      Lwt.return ()
    )
    "get after delete yields, which is A PROBLEM!"
    "get after delete fails, which was intended"

let _test_and_set_1 (client:client) =
  Lwt_log.info_f "_test_and_set_1" >>= fun () ->
  let wanted_s = "value!" in
  let wanted = Some wanted_s in
  client # test_and_set "test_and_set" None wanted >>= fun result ->
    begin
      match result with
	| None -> Llio.lwt_failfmt "result should not be None"
	| Some v ->
	  if v <> wanted_s
	  then
	    Lwt_log.info_f "value=%S, and should be '%s'" v wanted_s
	     >>= fun () ->
	  Llio.lwt_failfmt "different value"
	  else Lwt_log.info_f "value=%S, is what we expected" v
    end >>= fun () -> (* clean up *)
  client # delete "test_and_set" >>= fun () ->
  Lwt.return ()


let _test_and_set_2 (client: client) =
  Lwt_log.info_f "_test_and_set_2" >>= fun () ->
  let wanted = Some "wrong!" in
  client # test_and_set "test_and_set" (Some "x") wanted
  >>= fun result ->
  begin
    match result with
      | None -> Lwt_log.info_f "value is None, which is intended"
      | Some x -> Llio.lwt_failfmt "value='%S', which is unexpected" x
  end >>= fun () ->
  Lwt.return ()

let _test_and_set_3 (client: client) = 
  Lwt_log.info_f "_test_and_set_3" >>= fun () ->
  let key = "_test_and_set_3" in
  let value = "bla bla" in
  client # set key value >>= fun () ->
  client # test_and_set key (Some value) None >>= fun result ->
  begin 
    if result <> None then Llio.lwt_failfmt "should have been None" 
    else 
      begin
	client # exists key >>= fun b -> 
	if b then
	  Llio.lwt_failfmt "we should have deleted this"
	else Lwt.return ()
      end
  end
  
let _assert1 (client: client) =
  Lwt_log.info "_assert1" >>= fun () ->
  client # set "my_value" "my_value" >>= fun () ->
  client # aSSert "my_value" (Some "my_value") >>= fun () ->
  Lwt.return ()

let _assert2 (client: client) = 
  Lwt_log.info "_assert2" >>= fun () ->
  client # set "x" "x" >>= fun () ->
  should_fail 
    (fun () -> client # aSSert "x" (Some "y"))
    "PROBLEM:_assert2: yielded unit"
    "_assert2: ok, this aSSert should indeed fail"

let _assert3 (client:client) = 
  Lwt_log.info "_assert3" >>= fun () ->
  let k = "_assert3" in
  client # set k k >>= fun () ->
  Lwt_log.info "_assert3: value set" >>= fun () ->
  let updates = [
    Arakoon_client.Assert(k,Some k);
    Arakoon_client.Set(k, "REALLY")
  ]
  in
  client # sequence updates >>= fun () ->
  client # get k >>= fun v2 ->
  OUnit.assert_equal v2 "REALLY";
  Lwt.catch
    (fun () ->
      client # sequence updates >>= fun () ->
      let u2 = [
	Arakoon_client.Assert(k,Some k);
	Arakoon_client.Set(k,"NO WAY")
      ]
      in
      client # sequence u2)
  (function
    | Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED, msg) -> Lwt.return ()
    | ex -> Lwt.fail ex
  )
  >>= fun () ->
  client # get k >>= fun  v3 ->
  OUnit.assert_equal v2 "REALLY";
  Lwt.return ()

let _assert_exists1 (client: client) =
  Lwt_log.info "_assert_exists1" >>= fun () ->
  client # set "my_value_exists" "my_value_exists" >>= fun () ->
  client # aSSert_exists "my_value_exists" >>= fun () ->
  Lwt.return ()

let _assert_exists2 (client: client) = 
  Lwt_log.info "_assert_exists2" >>= fun () ->
  client # set "x_exists" "x_exists" >>= fun () ->
  should_fail 
    (fun () -> client # aSSert_exists "x_no_exists")
    "PROBLEM:_assert_exists2: yielded unit"
    "_assert_exists2: ok, this aSSert should indeed fail"

let _assert_exists3 (client:client) = 
  Lwt_log.info "_assert_exists3" >>= fun () ->
  let k = "_assert_exists3" in
  client # set k k >>= fun () ->
  Lwt_log.info "_assert_exists3: value set" >>= fun () ->
  let updates = [
    Arakoon_client.Assert_exists(k);
    Arakoon_client.Set(k, "REALLY")
  ]
  in
  client # sequence updates >>= fun () ->
  client # get k >>= fun v2 ->
  OUnit.assert_equal v2 "REALLY";
  Lwt.catch
    (fun () ->
      client # sequence updates >>= fun () ->
      let u2 = [
	Arakoon_client.Assert_exists(k);
	Arakoon_client.Set(k,"NO WAY")
      ]
      in
      client # sequence u2)
  (function
    | Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED, msg) -> Lwt.return ()
    | ex -> Lwt.fail ex
  )
  >>= fun () ->
  client # get k >>= fun  v3 ->
  OUnit.assert_equal v2 "REALLY";
  Lwt.return ()

let _range_1 (client: client) =
  Lwt_log.info_f "_range_1" >>= fun () ->
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
  Lwt_log.info_f "size = %i" size >>= fun () ->
  if size <> 10
  then Llio.lwt_failfmt "size should be 10 and is %i" size
  else Lwt.return ()

let _range_entries_1 (client: client) =
  Lwt_log.info_f "_range_entries_1" >>= fun () ->
  let rec fill i =
    if i = 100
    then Lwt.return ()
    else
      let key = "range_entries_" ^ (string_of_int i)
      and value = (string_of_int i) in
      client # set key value >>= fun () -> fill (i+1)
  in fill 0 >>= fun () ->
  client # range_entries (Some "range_entries") true (Some "rs") true 10 >>= fun keys ->
  let size = List.length keys in
  Lwt_log.info_f "size = %i" size >>= fun () ->
  if size <> 10
  then Llio.lwt_failfmt "size should be 10 and is %i" size
  else Lwt.return ()

let _detailed_range client =
  Lwt_log.info_f "_detailed_range" >>= fun () ->
  Arakoon_remote_client_test._test_range client

let _prefix_keys (client:client) =
  Lwt_log.info_f "_prefix_keys" >>= fun () ->
  Arakoon_remote_client_test._prefix_keys_test client

(* TODO: nodestream test 
let _list_entries (client:Nodestream.nodestream) =
  Lwt_log.info_f "_list_entries" >>= fun () ->
  let filename = "/tmp/_list_entries.tlog" in
  Lwt_io.with_file filename ~mode:Lwt_io.output
    (fun oc ->
      let f (i,update) = Lwt.return () in
      client # iterate Sn.start f)
*)
let _sequence (client: client) =
  Lwt_log.info_f "_sequence" >>= fun () ->
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
  Lwt_log.info_f "_sequence" >>= fun () ->
  let k1 = "I_DO_NOT_EXIST" in
  let k2 = "I_SHOULD_NOT_EXIST" in
  let updates = [
    Arakoon_client.Delete(k1);
    Arakoon_client.Set(k2, "REALLY")
  ]
  in
  should_fail
    (fun () -> client # sequence updates)
    "_sequence2:failing delete in sequence does not produce exception" 
    "_sequence2:produced exception, which is intended"
  >>= fun ()->
  Lwt_log.debug "_sequence2: part 2 of scenario" >>= fun () ->
  should_fail 
    (fun () -> client # get k2 >>= fun _ -> Lwt.return ())
    "PROBLEM:_sequence2: get yielded a value" 
    "_sequence2: ok, this get should indeed fail"
  >>= fun () -> Lwt.return ()

let _sequence3 (client: client) = 
  Lwt_log.info_f "_sequence3" >>= fun () ->
  let k1 = "sequence3:key1" 
  and k2 = "sequence3:key2" 
  in
  let changes = [Arakoon_client.Set (k1,k1 ^ ":value"); 
		 Arakoon_client.Delete k2;] in 
  should_fail 
    (fun () -> client # sequence changes) 
    "PROBLEM: _sequence3: change should fail (exception in change)" 
    "sequence3 changes indeed failed"
  >>= fun () ->
  should_fail 
    (fun () -> client # get k1 >>= fun v1 -> Lwt_log.info_f "value=:%s" v1)
    "PROBLEM: changes should be all or nothing" 
    "ok: all-or-noting changes"
  >>= fun () -> Lwt_log.info_f "sequence3.ok"

let _progress_possible (client:client) = 
  Lwt_log.info_f "_progress_possible" >>= fun () ->
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
	Lwt_io.printlf "v1=%S;v2=%S" v1 v2
	>>= fun () ->
	OUnit.assert_equal v1 key1;
	OUnit.assert_equal v2 key2;
	Lwt.return ()
	  | _ -> Lwt.fail (Failure "2 values expected")
  end >>= fun () ->
  Lwt.catch
    (fun () -> 
      client # multi_get ["I_DO_NOT_EXIST";key2] 
      >>= fun values ->
      Lwt.return ())
    (fun exn -> Lwt_log.debug ~exn "is this ok?")
  

let _with_master (cluster_cfg, _) f =
  let sp = float(cluster_cfg._lease_period) in
  Lwt_unix.sleep sp >>= fun () -> (* let the cluster reach stability *) 
  Client_main.find_master cluster_cfg >>= fun master_name ->
  Lwt_log.info_f "master=%S" master_name >>= fun () ->
  let master_cfg =
    List.hd 
      (List.filter (fun cfg -> cfg.node_name = master_name) cluster_cfg.cfgs)
  in
  Client_main.with_client master_cfg cluster_cfg.cluster_id f


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

let trivial_master4 tpl = _with_master tpl _multi_get 

let trivial_master5 tpl = _with_master tpl _progress_possible

let assert1 tpl = _with_master tpl _assert1

let assert2 tpl = _with_master tpl _assert2

let assert3 tpl = _with_master tpl _assert3

let assert_exists1 tpl = _with_master tpl _assert_exists1

let assert_exists2 tpl = _with_master tpl _assert_exists2

let assert_exists3 tpl = _with_master tpl _assert_exists3

let _node_name tn n = Printf.sprintf "%s_%i" tn n 

let setup make_master tn base () =
  let lease_period = 10 in
  let cluster_id = Printf.sprintf "%s_%i" tn base in
  let master = make_master tn 0 in
  let make_config () = Node_cfg.Node_cfg.make_test_config 
    ~base ~cluster_id    
    ~node_name:(_node_name tn)
    3 master lease_period 
  in
  let t0 = Node_main.test_t make_config (_node_name tn 0) >>= fun _ -> Lwt.return () in
  let t1 = Node_main.test_t make_config (_node_name tn 1) >>= fun _ -> Lwt.return () in
  let t2 = Node_main.test_t make_config (_node_name tn 2) >>= fun _ -> Lwt.return () in
  let all_t = [t0;t1;t2] in
  Lwt.return (make_config (), all_t)

let teardown (_, all_t) = Lwt.return ()

let make_suite base name w =
  let make_el tn b f = tn >:: w tn b f in
  name >:::
    [
      make_el "all_same_master" base all_same_master;
      (* "nothing_on_slave">:: w nothing_on_slave; *)
      make_el "dirty_on_slave"  (base +200) dirty_on_slave; 
      make_el "trivial_master"  (base +300) trivial_master; 
      make_el "trivial_master2" (base +400) trivial_master2;
      make_el "trivial_master3" (base +500) trivial_master3;
      make_el "trivial_master4" (base +600) trivial_master4;
      make_el "trivial_master5" (base +700) trivial_master5; 
      make_el "assert1"         (base + 800) assert1; 
      make_el "assert2"         (base + 900) assert2;
      make_el "assert3"         (base + 1000) assert3;
      make_el "assert_exists1"  (base + 1100) assert_exists1; 
      make_el "assert_exists2"  (base + 1200) assert_exists2;
      make_el "assert_exists3"  (base + 1300) assert_exists3; 
    ]

let force_master =
  let make_master tn n = Forced (_node_name tn  n) in
  let w tn base f = Extra.lwt_bracket (setup make_master tn base) f teardown in
  make_suite 4000 "force_master" w


let elect_master =
  let make_master tn _ = Elected in
  let w tn base f = Extra.lwt_bracket (setup make_master tn base) f teardown in
  make_suite 6000 "elect_master" w
