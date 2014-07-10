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

let section = Logger.Section.main

module TLSConfig : sig
    type t

    type path = string

    val make : ca_cert : path
             -> creds : (path * path) option
             -> protocol : Ssl.protocol
             -> t

    val ca_cert : t -> path
    val creds : t -> (path * path) option
    val protocol : t -> Ssl.protocol
end = struct
    type path = string

    type t = { ca_cert : path
             ; creds : (path * path) option
             ; protocol : Ssl.protocol
             }

    let make ~ca_cert ~creds ~protocol =
        { ca_cert; creds; protocol }

    let ca_cert t = t.ca_cert
    let creds t = t.creds
    let protocol t = t.protocol
end

let with_connection ~tls sa do_it = match tls with
  | None -> Lwt_io.with_connection sa do_it
  | Some tls_config ->
    let ctx = Typed_ssl.create_client_context (TLSConfig.protocol tls_config) in
    Typed_ssl.set_verify ctx
      [Ssl.Verify_peer; Ssl.Verify_fail_if_no_peer_cert]
      (Some Ssl.client_verify_callback);
    Typed_ssl.load_verify_locations ctx (TLSConfig.ca_cert tls_config) "";
    begin match (TLSConfig.creds tls_config) with
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
    Lwt.finalize
      (fun () -> do_it (ic, oc))
      (fun () -> Lwt_ssl.close sock)

exception Impossible of string

let with_connection' ~tls addrs f =
  let result = Lwt_mvar.create None
  and error = Lwt_mvar.create None
  and l = Lwt_mutex.create () in

  let f' addr =
    Lwt.catch
      (fun () ->
        with_connection ~tls addr (fun c ->
          if Lwt_mutex.is_locked l
            then Lwt.return_unit
            else Lwt_mutex.with_lock l (fun () ->
              Lwt_mvar.take result >>= function
                | Some _ as r -> Lwt_mvar.put result r
                | None -> begin
                    Lwt.catch
                      (fun () ->
                        f addr c >>= fun v ->
                        Lwt_mvar.put result (Some v))
                      (fun exn ->
                        Lwt_mvar.put result None >>= fun () ->
                        Lwt_mvar.take error >>= fun _ ->
                        Lwt_mvar.put error (Some exn))
                end)))
      (fun exn ->
        Lwt_mvar.take error >>= function
          | Some _ as v -> Lwt_mvar.put error v
          | None -> Lwt_mvar.put error (Some exn))
  in

  let ts = List.map f' addrs in
  Lwt.join ts >>= fun () ->
  Lwt_mvar.take result >>= function
    | Some v -> Lwt.return v
    | None -> Lwt_mvar.take error >>= function
        | None -> Lwt.fail (Impossible "Client_main.with_connection': 'result' & 'error' empty")
        | Some exn -> Lwt.fail exn


let with_client ~tls node_cfg (cluster:string) f =
  let addrs = List.map (fun ip -> make_address ip node_cfg.client_port) node_cfg.ips in
  let do_it _ connection =
    Arakoon_remote_client.make_remote_client cluster connection
    >>= fun (client: Arakoon_client.client) ->
    f client
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


(** Result type and utilities for {! find_master' } *)
module MasterLookupResult = struct
  open Node_cfg

  (** Result type for a master lookup using {! find_master' } *)
  type t = Found of Node_cfg.t (** Master found *)
         | No_master of Node_cfg.t list (** No master found after querying the provided list of nodes *)
         | Too_many_nodes_down of Node_cfg.t list (** Too many nodes down, more exactly the provided list *)
         | Unknown_node of (string * Node_cfg.t) (** An unknown node name was returned by a node *)
         | Exception of exn (** An exception occurred during lookup *)

  (** Create a {! string } representation of a {! MasterLookupResult.t } *)
  let to_string t =
    let open To_string in
    match t with
      | Found s -> Printf.sprintf "Found %s" (Node_cfg.string_of s)
      | No_master l -> Printf.sprintf "No_master %s" (list Node_cfg.string_of l)
      | Too_many_nodes_down l -> Printf.sprintf "Too_many_nodes_down %s" (list Node_cfg.string_of l)
      | Unknown_node (n, n') -> Printf.sprintf "Unknown_node (%S, %s)" n (Node_cfg.string_of n')
      | Exception exn -> Printf.sprintf "Exception (%s)" (Printexc.to_string exn)
end

(** Lookup a master node in a cluster

    The result is wrapped in a {! MasterLookupResult.t }, including (if
    applicable) exceptions, except {! Lwt.Canceled } which is passed through
    (you most likely don't want to catch that anyway).
*)
let find_master' ~tls cluster_cfg =
  let lookup_cfg n =
    let rec loop = function
      | [] -> None
      | (x :: xs) -> begin
          if x.node_name = n
            then Some x
            else loop xs
      end
    in
    loop (cluster_cfg.cfgs)
  in

  let open MasterLookupResult in

  let rec loop down unknown = function
    | [] -> begin
        let res =
          if unknown = []
            then
              Too_many_nodes_down down
            else
              No_master unknown
        in
        Logger.debug_f_
          "Client_main.find_master': %s" (MasterLookupResult.to_string res) >>= fun () ->
        Lwt.return res
    end
    | (cfg :: rest) -> begin
        Logger.debug_f_ "Client_main.find_master': Trying %S" cfg.node_name >>= fun () ->
        let addrs = List.map (fun ip -> make_address ip cfg.client_port) cfg.ips in
        Lwt.catch
          (fun () ->
            with_connection' ~tls addrs
              (fun _ connection ->
                Arakoon_remote_client.make_remote_client
                  cluster_cfg.cluster_id connection >>= fun client ->
                client # who_master ()) >>= function
                | None -> begin
                    Logger.debug_f_
                      "Client_main.find_master': %S doesn't know" cfg.node_name >>= fun () ->
                    loop down (cfg :: unknown) rest
                end
                | Some n -> begin
                    Logger.debug_f_ "Client_main.find_master': %S thinks %S" cfg.node_name n >>= fun () ->
                    if n = cfg.node_name
                      then begin
                        let res = Found cfg in
                        Logger.debug_f_ "Client_main.find_master': %s" (to_string res) >>= fun () ->
                        return res
                      end
                      else
                        match lookup_cfg n with
                          | Some ncfg ->
                              loop down unknown (ncfg :: rest)
                          | None -> begin
                              let res = Unknown_node (n, cfg) in
                              Logger.warning_f_
                                "Client_main.find_master': %s" (to_string res) >>= fun () ->
                              return res
                          end
                end)
          (function
            | Unix.Unix_error(Unix.ECONNREFUSED, _, _) ->
                Logger.debug_f_
                  "Client_main.find_master': Connection to %S refused" cfg.node_name >>= fun () ->
                loop (cfg :: down) unknown rest
            | Lwt.Canceled as exn ->
                Lwt.fail exn
            | exn -> begin
                let res = Exception exn in
                Logger.debug_f_
                  "Client_main.find_master': %s" (to_string res) >>= fun () ->
                return res
            end)
    end
  in
  loop [] [] (cluster_cfg.cfgs)

let find_master ~tls cluster_cfg =
  let open MasterLookupResult in
  find_master' ~tls cluster_cfg >>= function
    | Found m -> return m.node_name
    | No_master _ -> Lwt.fail (Failure "No Master")
    | Too_many_nodes_down _ -> Lwt.fail (Failure "too many nodes down")
    | Unknown_node (n, _) -> return n (* Keep original behaviour *)
    | Exception exn -> Lwt.fail exn

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
