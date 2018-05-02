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

open Node_cfg
open Network
open Statistics
open Client_helper
open Lwt

let default_create_client_context = default_create_client_context
let with_connection = with_connection
let with_connection' = with_connection'


let with_remote_nodestream ~tls ~tcp_keepalive node_cfg cluster_id f =
  let open Node_cfg in
  let addrs = List.map (fun ip -> make_address ip node_cfg.client_port) node_cfg.ips in
  let do_it _addr connection =
    Remote_nodestream.make_remote_nodestream cluster_id connection >>= f
  in
  with_connection' ~tls ~tcp_keepalive addrs do_it

let run f = Lwt_extra.run f ; 0

let ping ~tls ~tcp_keepalive ip port cluster_id =
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
  let t () = with_connection ~tls ~tcp_keepalive sa do_it in
  run t


let find_master ?tls cluster_cfg =
  let open MasterLookupResult in
  find_master' ?tls cluster_cfg >>= function
    | Found (node_name, _) -> return node_name
    | No_master -> Lwt.fail (Failure "No Master")
    | Too_many_nodes_down -> Lwt.fail (Failure "too many nodes down")
    | Unknown_node (n, _) -> return n (* Keep original behaviour *)
    | Exception exn -> Lwt.fail exn

let retrieve_cfg cfg_url =
  Arakoon_config_url.retrieve cfg_url >|= Arakoon_client_config.from_ini

let with_master_client ~tls cfg_url f =
  retrieve_cfg cfg_url >>= fun ccfg ->
  find_master ?tls ccfg >>= fun master_name ->
  with_client'' ?tls ccfg master_name f

let set ~tls cfg_name key value_option =
  let t () =
    (match value_option with
    | Some value -> Lwt.return value
    | None -> Lwt_io.read Lwt_io.stdin
    ) >>= fun value ->
    with_master_client ~tls cfg_name (fun client -> client # set key value)
  in run t

let get ?(raw=false)~tls cfg_name key =
  let f (client:Arakoon_client.client) =
    client # get key >>= fun value ->
    if raw
    then Lwt_io.print value
    else Lwt_io.printlf "%S%!" value
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

let nop ~tls cfg_name =
  let t () =
    with_master_client
      ~tls cfg_name
      (fun client -> client # nop ())
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

let who_master ~tls cfg_url () =

  let t () =
    retrieve_cfg cfg_url >>= fun cluster_cfg ->
    find_master ?tls cluster_cfg >>= fun master_name ->
    Lwt_io.printl master_name
  in
  run t

let node_state ~tls node_name cfg_url =
  let t () =
    retrieve_cfg cfg_url >>= fun ccfg ->
    with_client''
      ?tls
      ccfg node_name
      (fun client ->
       client # current_state () >>= fun state ->
       Lwt_io.printl state
      )
  in
  run t


let node_version ~tls node_name cfg_url =
  let t () =
    retrieve_cfg cfg_url >>= fun ccfg ->
    with_client''
      ?tls
      ccfg node_name
      (fun client ->
       client # version () >>= fun (major,minor,patch, info) ->
       Lwt_io.printlf "%i.%i.%i" major minor patch >>= fun () ->
       Lwt_io.printl info
      )
  in
  run t
