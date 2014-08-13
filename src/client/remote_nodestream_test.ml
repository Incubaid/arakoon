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

open Lwt
open OUnit
open Remote_nodestream
open Node_cfg
open Arakoon_remote_client
open Routing
open Interval

let setup port = Lwt.return port

let teardown () = Lwt.return ()

let section = Logger.Section.main

let cluster_id = "baby1"

let __wrap__ port conversation =
  let stop = ref false in
  let lease_period = 2 in
  let make_config () = Node_cfg.make_test_config
                         ~cluster_id
                         ~node_name:(Printf.sprintf "sweety_%i")
                         ~base:port
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
    let address = Unix.ADDR_INET (Unix.inet_addr_loopback, master_cfg.Node_cfg.client_port) in
    Lwt_io.open_connection address >>= fun (ic,oc) ->
    conversation (ic, oc) >>= fun () ->
    Logger.debug section "end_of_senario" >>= fun () ->
    Lwt_io.close ic >>= fun () ->
    Lwt_io.close oc >>= fun () ->
    Lwt.return ()
  in
  Lwt.pick [client_t ();t0] >>= fun () ->
  stop := true;
  Lwt.return ()

let set_interval port () =
  let conversation conn =
    Logger.debug_ "starting set_interval ..." >>= fun () ->
    Common.prologue cluster_id conn >>= fun () ->
    let i0 = Interval.make (Some "a") None None None in
    Logger.debug_f_ "i0=%S" (Interval.to_string i0) >>= fun () ->
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
        make_remote_client cluster_id conn >>= fun client ->
        Lwt_list.iter_s (fun (k,v) -> client # set k v)
          [("k1", "vk1");
           ("k2", "vk2");
           ("p1", "vp1");
           ("a" , "va");
          ] >>= fun () ->
        client # get "k1" >>= fun v ->
        Logger.debug_f_ "a[%s] = %s" "k1" v >>= fun () ->
        Lwt.return ()
      )
  in
  let conversation conn =
    fill_it_a_bit ()  >>= fun () ->
    let (ic,oc) = conn in
    make_remote_nodestream cluster_id conn >>= fun ns ->
    Logger.debug_ "starting get_fringe" >>= fun () ->
    ns # get_fringe (Some "k") Routing.LOWER_BOUND >>= fun kvs ->
    let got = List.length kvs in
    Logger.debug_f_ "got: %i" got >>= fun () ->
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
    make_remote_nodestream cluster_id conn >>= fun ns ->
    Logger.debug_ "starting set_routing" >>= fun () ->
    ns # set_routing r >>= fun () ->
    Logger.debug_ "starting set_routing_delta" >>= fun () ->
    ns # set_routing_delta "left" "l" "right" >>= fun () ->
    Logger.debug_ "starting get_routing" >>= fun () ->
    ns # get_routing () >>= fun new_r ->
    let new_ser = Buffer.create 15 in
    Routing.routing_to new_ser new_r;
    let old_str = Buffer.contents old_ser in
    let new_str = Buffer.contents new_ser in
    Logger.debug_f_ "old_str: %s " old_str >>= fun () ->
    Logger.debug_f_ "new_str: %s" new_str  >>= fun () ->
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
