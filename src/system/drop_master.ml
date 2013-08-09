open Lwt
open OUnit
open Master_type
open Node_cfg.Node_cfg

let section = Logger.Section.main

let setup tn master base () =
  let lease_period = 10 in
  let make_config () = Node_cfg.Node_cfg.make_test_config ~base 3 master lease_period in
  let t0 = Node_main.test_t make_config "t_arakoon_0" >>= fun _ -> Lwt.return () in
  let t1 = Node_main.test_t make_config "t_arakoon_1" >>= fun _ -> Lwt.return () in
  let t2 = Node_main.test_t make_config "t_arakoon_2" >>= fun _ -> Lwt.return () in
  let all_t = [t0;t1;t2] in
  Lwt.return (tn, make_config (), all_t)

let teardown (tn, _, all_t) = Lwt.return ()

let _with_master_admin (tn, cluster_cfg, _) f =
  let sp = float(cluster_cfg._lease_period) *. 1.2 in
  Lwt_unix.sleep sp >>= fun () -> (* let the cluster reach stability *)
  Client_main.find_master cluster_cfg >>= fun master_name ->
  Logger.info_f_ "master=%S" master_name >>= fun () ->
  let master_cfg =
    List.hd
      (List.filter (fun cfg -> cfg.node_name = master_name) cluster_cfg.cfgs)
  in
  let host,port = List.hd master_cfg.ips , master_cfg.client_port in
  let sa = Network.make_address host port in
  let cid = cluster_cfg.cluster_id in
  Lwt_io.with_connection sa
    (fun conn ->
       Remote_nodestream.make_remote_nodestream cid conn >>= fun admin ->
       f cluster_cfg master_name admin
    )

let _drop_master cluster_cfg master_name admin =
  Logger.info_ "drop_master scenario" >>= fun () ->
  admin # drop_master () >>= fun () ->
  Client_main.find_master cluster_cfg >>= fun new_master ->
  Logger.info_f_ "new? master = %s" new_master >>= fun () ->
  OUnit.assert_bool "master should have been changed" (new_master <> master_name);
  Lwt.return ()

let drop_master tpl = _with_master_admin tpl _drop_master



let make_suite base name w =
  let make_el n base f = n >:: w n base f in
  name >:::
    [make_el "drop_master" base drop_master;
    ]


let suite =
  let w tn base f = Extra.lwt_bracket (setup tn Elected base) f teardown in
  make_suite 8000 "drop_master" w
