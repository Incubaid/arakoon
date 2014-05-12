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
open Lwt

let _address_to_use ips port = make_address (List.hd ips) port

let with_connection ~tls sa do_it = match tls with
  | None -> Lwt_io.with_connection sa do_it
  | Some (tls_ca_cert, tls_creds) ->
    let ctx = Typed_ssl.create_client_context Ssl.TLSv1 in
    Typed_ssl.set_verify ctx
      [Ssl.Verify_peer; Ssl.Verify_fail_if_no_peer_cert]
      (Some Ssl.client_verify_callback);
    Typed_ssl.load_verify_locations ctx tls_ca_cert "";
    begin match tls_creds with
      | None -> ()
      | Some (cert, key) ->
          Typed_ssl.use_certificate ctx cert key
    end;
    let fd = Lwt_unix.socket (Unix.domain_of_sockaddr sa) Unix.SOCK_STREAM 0 in
    Lwt_unix.set_close_on_exec fd;
    Lwt_unix.connect fd sa >>= fun () ->
    Typed_ssl.Lwt.ssl_connect fd ctx >>= fun (_, sock) ->
    let ic = Lwt_ssl.in_channel_of_descr sock
    and oc = Lwt_ssl.out_channel_of_descr sock in
    Lwt.catch
      (fun () ->
        do_it (ic, oc) >>= fun r ->
        Lwt_ssl.close sock >>= fun () ->
        Lwt.return r)
      (fun exc ->
        Lwt_ssl.close sock >>= fun () ->
        Lwt.fail exc)


let with_client ~tls node_cfg (cluster:string) f =
  let sa = _address_to_use node_cfg.ips node_cfg.client_port in
  let do_it connection =
    Arakoon_remote_client.make_remote_client cluster connection
    >>= fun (client: Arakoon_client.client) ->
    f client
  in
  with_connection ~tls sa do_it

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
  let cfgs = cluster_cfg.cfgs in
  let rec loop = function
    | [] -> Lwt.fail (Failure "too many nodes down")
    | cfg :: rest ->
      begin
        let section = Logger.Section.main in
        Logger.info_f_ "cfg=%s" cfg.node_name >>= fun () ->
        let sa = _address_to_use cfg.ips cfg.client_port in
        Lwt.catch
          (fun () ->
             with_connection ~tls sa
               (fun connection ->
                  Arakoon_remote_client.make_remote_client
                    cluster_cfg.cluster_id connection
                  >>= fun client ->
                  client # who_master ())
             >>= function
             | None -> Lwt.fail (Failure "No Master")
             | Some m -> Logger.info_f_ "master=%s" m >>= fun () ->
               Lwt.return m)
          (function
            | Unix.Unix_error(Unix.ECONNREFUSED,_,_ ) ->
              Logger.info_f_ "node %s is down, trying others" cfg.node_name >>= fun () ->
              loop rest
            | exn -> Lwt.fail exn
          )
      end
  in loop cfgs

let run f = Lwt_main.run (f ()); 0

let with_master_client ~tls cfg_name f =
  let ccfg = read_config cfg_name in
  let cfgs = ccfg.cfgs in
  find_master ~tls ccfg >>= fun master_name ->
  let master_cfg = List.hd (List.filter (fun cfg -> cfg.node_name = master_name) cfgs) in
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
       client # range_entries left linc right rinc max_results >>= fun entries ->
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
       client # rev_range_entries left linc right rinc max_results >>= fun entries ->
       Lwt_list.iter_s (fun (k,v) -> Lwt_io.printlf "%S %S" k v ) entries >>= fun () ->
       let size = List.length entries in
       Lwt_io.printlf "%i listed" size >>= fun () ->
       Lwt.return ()
      )
  in
  run t

let benchmark ~tls cfg_name key_size value_size tx_size max_n n_clients =
  Lwt_io.set_default_buffer_size 32768;
  let t () =
    let with_c = with_master_client ~tls cfg_name in
    Benchmark.benchmark ~with_c ~key_size ~value_size ~tx_size ~max_n n_clients
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

let _cluster_and_node_cfg node_name cfg_name =
  let cluster_cfg = read_config cfg_name in
  let _find cfgs =
    let rec loop = function
      | [] -> failwith (node_name ^ " is not known in config " ^ cfg_name)
      | cfg :: rest ->
        if cfg.node_name = node_name then cfg
        else loop rest
    in
    loop cfgs
  in
  let node_cfg = _find cluster_cfg.cfgs in
  cluster_cfg, node_cfg

let node_state ~tls node_name cfg_name =
  let cluster_cfg,node_cfg = _cluster_and_node_cfg node_name cfg_name in
  let cluster = cluster_cfg.cluster_id in
  let f client =
    client # current_state () >>= fun state ->
    Lwt_io.printl state
  in
  let t () = with_client ~tls node_cfg cluster f in
  run t



let node_version ~tls node_name cfg_name =
  let cluster_cfg, node_cfg = _cluster_and_node_cfg node_name cfg_name in
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
