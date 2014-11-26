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

open Node_cfg.Node_cfg
open Network
open Statistics
open Client_helper
open Lwt

let default_create_client_context = default_create_client_context
let with_connection = with_connection
let with_connection' = with_connection'

let with_client ~tls node_cfg cluster_id f =
  with_client' ~tls (node_cfg_to_node_client_cfg node_cfg) cluster_id f

let with_remote_nodestream ~tls node_cfg cluster_id f =
  let open Node_cfg in
  let addrs = List.map (fun ip -> make_address ip node_cfg.client_port) node_cfg.ips in
  let do_it _addr connection =
    Remote_nodestream.make_remote_nodestream cluster_id connection >>= f
  in
  with_connection' ~tls addrs do_it

let ping ~tls ip port cluster_id =
  let do_it connection =
    let t0 = Unix.gettimeofday () in
    Arakoon_remote_client.make_remote_client cluster_id connection
    >>= fun (client: Arakoon_client.client) ->
    client # ping "cucu" cluster_id >>= fun s ->
    let t1 = Unix.gettimeofday() in
    let d = t1 -. t0 in
    Lwt_io.printlf "%s\ntook\t%f" s d
  in
  let sa = make_address ip port in
  let t = with_connection ~tls sa do_it in
  Lwt_main.run t; 0




let find_master ~tls cluster_cfg =
  let cluster_cfg' = to_client_cfg cluster_cfg in
  let open MasterLookupResult in
  find_master' ~tls cluster_cfg' >>= function
    | Found (node_name, _) -> return node_name
    | No_master -> Lwt.fail (Failure "No Master")
    | Too_many_nodes_down -> Lwt.fail (Failure "too many nodes down")
    | Unknown_node (n, _) -> return n (* Keep original behaviour *)
    | Exception exn -> Lwt.fail exn

let run f = Lwt_main.run (f ()); 0

let with_master_client ~tls cfg_name f =
  let open Node_cfg in
  let ccfg = read_config cfg_name in
  find_master ~tls ccfg >>= fun master_name ->
  let master_cfg = List.hd (List.filter (fun cfg -> cfg.node_name = master_name) ccfg.cfgs) in
  with_client ~tls master_cfg ccfg.cluster_id f

let set ~tls cfg_name key value =
  let t () = with_master_client ~tls cfg_name (fun client -> client # set key value)
  in run t

let get ~tls cfg_name key =
  let f (client:Arakoon_client.client) =
    client # get key >>= fun value ->
    Lwt_io.printlf "%S%!" value
  in
  let t () = with_master_client ~tls cfg_name f in
  run t


let get_key_count ~tls cfg_name () =
  let f (client:Arakoon_client.client) =
    client # get_key_count () >>= fun c64 ->
    Lwt_io.printlf "%Li%!" c64
  in
  let t () = with_master_client ~tls cfg_name f in
  run t

let delete ~tls cfg_name key =
  let t () = with_master_client ~tls cfg_name (fun client -> client # delete key )
  in
  run t

let delete_prefix ~tls cfg_name prefix =
  let t () = with_master_client ~tls cfg_name (
      fun client -> client # delete_prefix prefix >>= fun n_deleted ->
        Lwt_io.printlf "%i" n_deleted
    )
  in
  run t

let prefix ~tls cfg_name prefix prefix_size =
  let t () = with_master_client ~tls cfg_name
               (fun client ->
                  client # prefix_keys prefix prefix_size >>= fun keys ->
                  Lwt_list.iter_s (fun k -> Lwt_io.printlf "%S" k ) keys >>= fun () ->
                  Lwt.return ()
               )
  in
  run t

let range_entries ~tls cfg_name left linc right rinc max_results =
  let t () =
    with_master_client ~tls
      cfg_name
      (fun client ->
        client # range_entries ~consistency:Arakoon_client.Consistent ~first:left ~finc:linc ~last:right ~linc:rinc ~max:max_results >>= fun entries ->
       Lwt_list.iter_s (fun (k,v) -> Lwt_io.printlf "%S %S" k v ) entries >>= fun () ->
       let size = List.length entries in
       Lwt_io.printlf "%i listed" size >>= fun () ->
       Lwt.return ()
      )
  in
  run t

let rev_range_entries ~tls cfg_name left linc right rinc max_results =
  let t () =
    with_master_client ~tls
      cfg_name
      (fun client ->
       client # rev_range_entries ~consistency:Arakoon_client.Consistent ~first:left ~finc:linc ~last:right ~linc:rinc ~max:max_results >>= fun entries ->
       Lwt_list.iter_s (fun (k,v) -> Lwt_io.printlf "%S %S" k v ) entries >>= fun () ->
       let size = List.length entries in
       Lwt_io.printlf "%i listed" size >>= fun () ->
       Lwt.return ()
      )
  in
  run t

let user_function ~tls cfg_name name arg =
  let t () =
    with_master_client ~tls
      cfg_name
      (fun client ->
       client # user_function name arg >>= fun res ->
       Lwt_io.printlf "res = %s" (Log_extra.string_option2s res) >>= fun () ->
       Lwt.return ()
      )
  in
  run t

let benchmark
      ~tls
      cfg_name key_size value_size tx_size max_n n_clients
      scenario_s =
  Lwt_io.set_default_buffer_size 32768;
  let scenario = Ini.p_string_list scenario_s in
  let t () =
    let with_c = with_master_client ~tls cfg_name in
    Benchmark.benchmark
      ~with_c ~key_size ~value_size ~tx_size ~max_n n_clients scenario
  in
  run t


let expect_progress_possible ~tls cfg_name =

  let f client =
    client # expect_progress_possible () >>= fun b ->
    Lwt_io.printlf "%b" b
  in
  let t () = with_master_client ~tls cfg_name f
  in
  run t


let statistics ~tls cfg_name =
  let f client =
    client # statistics () >>= fun statistics ->
    let rep = Statistics.string_of statistics in
    Lwt_io.printl rep
  in
  let t () = with_master_client ~tls cfg_name f
  in run t

let who_master ~tls cfg_name () =
  let cluster_cfg = read_config cfg_name in
  let t () =
    find_master ~tls cluster_cfg >>= fun master_name ->
    Lwt_io.printl master_name
  in
  run t

let _cluster_and_node_cfg node_name' cfg_name =
  let open Node_cfg in
  let cluster_cfg = read_config cfg_name in
  let _find cfgs =
    let rec loop = function
      | [] -> failwith (node_name' ^ " is not known in config " ^ cfg_name)
      | cfg :: rest ->
        if cfg.node_name = node_name' then cfg
        else loop rest
    in
    loop cfgs
  in
  let node_cfg = _find cluster_cfg.cfgs in
  cluster_cfg, node_cfg

let node_state ~tls node_name' cfg_name =
  let open Node_cfg in
  let cluster_cfg,node_cfg = _cluster_and_node_cfg node_name' cfg_name in
  let cluster = cluster_cfg.cluster_id in
  let f client =
    client # current_state () >>= fun state ->
    Lwt_io.printl state
  in
  let t () = with_client ~tls node_cfg cluster f in
  run t



let node_version ~tls node_name' cfg_name =
  let open Node_cfg in
  let cluster_cfg, node_cfg = _cluster_and_node_cfg node_name' cfg_name in
  let cluster = cluster_cfg.cluster_id in
  let t () =
    with_client ~tls node_cfg cluster
      (fun client ->
         client # version () >>= fun (major,minor,patch, info) ->
         Lwt_io.printlf "%i.%i.%i" major minor patch >>= fun () ->
         Lwt_io.printl info
      )
  in
  run t
