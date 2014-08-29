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

open Std
open Lwt
open OUnit
open Master_type
open Client_config
open Node_cfg
open Node_cfg

let section = Logger.Section.main

let stop = ref (ref false)

let setup tn master base () =
  let lease_period = 2 in
  let stop = !stop in
  let make_config () = Node_cfg.make_test_config ~base 3 master lease_period in
  let t0 = Node_main.test_t make_config "t_arakoon_0" ~stop >>= fun _ -> Lwt.return () in
  let t1 = Node_main.test_t make_config "t_arakoon_1" ~stop >>= fun _ -> Lwt.return () in
  (* let t2 = Node_main.test_t make_config "t_arakoon_2" stop >>= fun _ -> Lwt.return () in *)
  let all_t = [t0;t1(* ;t2 *)] in
  Lwt.return (tn, make_config (), all_t)

let find_master_exn cluster_cfg =
  Client_main.find_master_exn ~tls:None (Node_cfg.to_client_config cluster_cfg)

let wait_until_master cluster_cfg =
  Logger.debug_ "waiting until there's a master..." >>= fun () ->
  Lwt_unix.with_timeout
    60.
    (fun () ->
     Client_main.find_master_loop ~tls:None
                                  (Node_cfg.to_client_config cluster_cfg))
  >>= function
    | Result.Ok master_cfg -> Lwt.return master_cfg.NodeConfig.name
    | Result.Error err -> Lwt.fail (Client_main.Master_lookup_error err)

let teardown (_tn, _, all_t) =
  !stop := true;
  stop := ref false;
  Lwt.join all_t

let _drop_master do_maintenance (_tn, cluster_cfg, _) =
  let lease_period = cluster_cfg.Node_cfg._lease_period in
  let sp = (float lease_period) *. 1.2 in
  Lwt_unix.sleep sp >>= fun () -> (* let the cluster reach stability *)
  Client_main.find_master_exn' ~tls:None (Node_cfg.to_client_config cluster_cfg) >>= fun master_name ->
  if do_maintenance
  then
    begin
      let slave = List.hd (List.filter ((<>) master_name) ["t_arakoon_0"; "t_arakoon_1"]) in
      let slave_cfg = List.hd (List.filter (fun cfg -> cfg.Node_cfg.node_name = slave) cluster_cfg.Node_cfg.cfgs) in
      let open Remote_nodestream in
      let open Network in
      let address = make_address (List.hd slave_cfg.ips) slave_cfg.client_port in
      Lwt.ignore_result
        (Lwt_io.with_connection
           address
           (fun connection ->
            make_remote_nodestream cluster_cfg.cluster_id connection >>= fun client ->
            client # defrag_db ()))
    end;
  Logger.info_f_ "master=%S" master_name >>= fun () ->
  let master_cfg =
    List.hd
      (List.filter (fun cfg -> cfg.node_name = master_name) cluster_cfg.cfgs)
  in
  let host,port = List.hd master_cfg.ips , master_cfg.client_port in
  let sa = Network.make_address host port in
  let cid = cluster_cfg.cluster_id in
  let ccfg = Node_cfg.to_client_config cluster_cfg in
  Lwt.pick
    [(Lwt_io.with_connection sa
                             (fun conn ->
                              Remote_nodestream.make_remote_nodestream cid conn >>= fun client ->
                              Logger.info_ "drop_master scenario" >>= fun () ->
                              client # drop_master () >>= fun () ->
                              if not do_maintenance
                              then
                                begin
                                  Client_main.find_master_exn' ~tls:None ccfg >>= fun new_master ->
                                  Logger.info_f_ "new? master = %s" new_master >>= fun () ->
                                  OUnit.assert_bool "master should have been changed" (new_master <> master_name);
                                  Lwt.return ()
                                end
                              else
                                  wait_until_master cluster_cfg >>= fun new_master ->
                                  Logger.info_f_ "new? master = %s" new_master >>= fun () ->
                                  OUnit.assert_bool "master should be the same" (new_master = master_name);
                                  Lwt.return ()));
     (Lwt_unix.sleep 60. >>= fun () ->
      Lwt.fail (Failure "drop master did not terminate quickly enough"))] >>= fun () ->
  if do_maintenance
  then
    Lwt_condition.signal Mem_store.defrag_condition ();
  Lwt_unix.sleep (float lease_period) >>= fun () ->
  find_master_exn cluster_cfg >>= fun _ ->
  Lwt_unix.sleep (float lease_period) >>= fun () ->
  find_master_exn cluster_cfg >>= fun _ ->
  Lwt.return ()

let drop_master tpl = _drop_master false tpl
let drop_master_while_maintenance tpl = _drop_master true tpl


let make_suite base name w =
  let make_el n base f = n >:: w n base f in
  name >:::
    [make_el "drop_master" base drop_master;
     make_el "drop_master_while_maintenance" (base + 100) drop_master_while_maintenance;
    ]


let suite =
  let w tn base f = Extra.lwt_bracket (setup tn Elected base) f teardown in
  make_suite 8000 "drop_master" w
