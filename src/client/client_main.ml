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
open Client_config
open Network
open Statistics
open Lwt


open Lwt_extra

let section = Logger.Section.main

let default_create_client_context ~ca_cert ~creds ~protocol =
  let ctx = Typed_ssl.create_client_context protocol in
  Typed_ssl.set_verify ctx
                       [Ssl.Verify_peer; Ssl.Verify_fail_if_no_peer_cert]
                       (Some Ssl.client_verify_callback);
  Typed_ssl.load_verify_locations ctx ca_cert "";
  begin
    match creds with
    | None -> ()
    | Some (cert, key) ->
       Typed_ssl.use_certificate ctx cert key
  end;
  ctx


let with_connection ~tls sa do_it = match tls with
  | None -> Lwt_io.with_connection sa do_it
  | Some ctx ->
    let fd = Lwt_unix.socket (Unix.domain_of_sockaddr sa) Unix.SOCK_STREAM 0 in
    Lwt_unix.set_close_on_exec fd;
    Lwt_unix.connect fd sa >>= fun () ->
    Typed_ssl.Lwt.ssl_connect fd ctx >>= fun (_, sock) ->
    let ic = Lwt_ssl.in_channel_of_descr sock
    and oc = Lwt_ssl.out_channel_of_descr sock in
    Lwt.finalize
      (fun () -> do_it (ic, oc))
      (fun () -> Lwt_ssl.close sock)


exception No_connection

let with_connection' ~tls addrs f =
  let count = List.length addrs in
  let res = Lwt_mvar.create_empty () in
  let err = Lwt_mvar.create None in
  let l = Lwt_mutex.create () in
  let cd = CountDownLatch.create ~count in

  let f' addr =
    Lwt.catch
      (fun () ->
        with_connection ~tls addr (fun c ->
          if Lwt_mutex.is_locked l
          then Lwt.return ()
          else
          Lwt_mutex.lock l >>= fun () ->
            Lwt.catch
              (fun () -> f addr c >>= fun v -> Lwt_mvar.put res (`Success v))
              (fun exn -> Lwt_mvar.put res (`Failure exn))))
      (fun exn ->
        Lwt.protected (
          Logger.warning_f_ ~exn "Failed to connect to %s" (Network.a2s addr) >>= fun () ->
          Lwt_mvar.take err >>= begin function
            | Some _ as v -> Lwt_mvar.put err v
            | None -> Lwt_mvar.put err (Some exn)
          end >>= fun () ->
          CountDownLatch.count_down cd) >>= fun () ->
        Lwt.fail exn)
  in

  let ts = List.map f' addrs in
  Lwt.finalize
    (fun () ->
      Lwt.pick [ Lwt_mvar.take res >>= begin function
                   | `Success v -> Lwt.return v
                   | `Failure exn -> Lwt.fail exn
                 end
               ; CountDownLatch.await cd >>= fun () ->
                 Lwt_mvar.take err >>= function
                   | None -> Lwt.fail No_connection
                   | Some e -> Lwt.fail e
               ]
    )
    (fun () ->
      Lwt_list.iter_p (fun t ->
        let () = try
          Lwt.cancel t
        with _ -> () in
        Lwt.return ())
        ts)


let with_client ~tls node_cfg cluster_id f =
  let open NodeConfig in
  let addrs = List.map (fun ip -> make_address ip node_cfg.port) node_cfg.ips in
  let do_it _addr connection =
    Arakoon_remote_client.make_remote_client cluster_id connection >>= f
  in
  with_connection' ~tls addrs do_it

let with_remote_nodestream ~tls node_cfg cluster_id f =
  let open NodeConfig in
  let addrs = List.map (fun ip -> make_address ip node_cfg.port) node_cfg.ips in
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


(** Result type and utilities for {! find_master' } *)
module MasterLookupError = struct

  (** Result type for a master lookup using {! find_master' } *)
  type t =
    | No_master of NodeConfig.t list (** No master found after querying the provided list of nodes *)
    | Too_many_nodes_down of NodeConfig.t list (** Too many nodes down, more exactly the provided list *)
    | Unknown_node of (string * NodeConfig.t) (** An unknown node name was returned by a node *)
    | Exception of exn (** An exception occurred during lookup *)

  (** Create a {! string } representation of a {! MasterLookupResult.t } *)
  let to_string t =
    let open To_string in
    match t with
      | No_master l -> Printf.sprintf "No_master %s" (list NodeConfig.show l)
      | Too_many_nodes_down l -> Printf.sprintf "Too_many_nodes_down %s" (list NodeConfig.show l)
      | Unknown_node (n, n') -> Printf.sprintf "Unknown_node (%S, %s)" n (NodeConfig.show n')
      | Exception exn -> Printf.sprintf "Exception (%s)" (Printexc.to_string exn)
end

(** Lookup a master node in a cluster

    The result is wrapped in a {! MasterLookupResult.t }, including (if
    applicable) exceptions, except {! Lwt.Canceled } which is passed through
    (you most likely don't want to catch that anyway).
*)
let find_master ~tls cluster_cfg =
  let open NodeConfig in
  let open ClusterClientConfig in
  let lookup_cfg n =
    let rec loop = function
      | [] -> None
      | (x :: xs) -> begin
          if x.name = n
            then Some x
            else loop xs
      end
    in
    loop (cluster_cfg.nodes)
  in

  let open MasterLookupError in

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
          "Client_main.find_master': %s" (MasterLookupError.to_string res) >>= fun () ->
        Lwt.return (Result.Error res)
    end
    | (cfg :: rest) -> begin
        Logger.debug_f_ "Client_main.find_master': Trying %S" cfg.name >>= fun () ->
        Lwt.catch
          (fun () ->
            with_client
              ~tls
              cfg cluster_cfg.id
              (fun client ->
                client # who_master ()) >>= function
                | None -> begin
                    Logger.debug_f_
                      "Client_main.find_master': %S doesn't know" cfg.name >>= fun () ->
                    loop down (cfg :: unknown) rest
                end
                | Some n -> begin
                    Logger.debug_f_ "Client_main.find_master': %S thinks %S" cfg.name n >>= fun () ->
                    if n = cfg.name
                      then begin
                        Logger.debug_f_ "Client_main.find_master': %s" (NodeConfig.show cfg) >>= fun () ->
                        return (Result.Ok cfg)
                      end
                      else
                        match lookup_cfg n with
                          | Some ncfg ->
                              loop down unknown (ncfg :: rest)
                          | None -> begin
                              let res = Unknown_node (n, cfg) in
                              Logger.warning_f_
                                "Client_main.find_master': %s" (to_string res) >>= fun () ->
                              return (Result.Error res)
                          end
                end)
          (function
            | Unix.Unix_error(Unix.ECONNREFUSED, _, _) ->
                Logger.debug_f_
                  "Client_main.find_master': Connection to %S refused" cfg.name >>= fun () ->
                loop (cfg :: down) unknown rest
            | Lwt.Canceled as exn ->
                Lwt.fail exn
            | exn -> begin
                let res = Exception exn in
                Logger.debug_f_
                  "Client_main.find_master': %s" (to_string res) >>= fun () ->
                return (Result.Error res)
            end)
    end
  in
  loop [] [] (cluster_cfg.nodes)


(** Lookup the master of a cluster in a loop

    This action calls {! find_master' } in a loop, as long as it returns
    {! MasterLookupResult.No_master } or
    {! MasterLookupResult.Too_many_nodes_down }. Other return values are passed
    through to the caller.

    In some circumstances, this action could loop forever, so it's mostly
    useful in combination with {! Lwt_unix.with_timeout } or something related.
*)
let find_master_loop ~tls cluster_cfg =
  let rec loop () =
    let open Result in
    find_master ~tls cluster_cfg >>=
      function
      | (Ok _ as r) -> return r
      | (Error e as r) ->
         let open MasterLookupError in
         begin match e with
               | No_master _
               | Too_many_nodes_down _ ->
                  Logger.debug_f_ "Client_main.find_master_loop: %s" (MasterLookupError.to_string e)
                  >>= loop
               | Unknown_node _
               | Exception _ -> return r
         end
  in
  loop ()

exception Master_lookup_error of MasterLookupError.t

let find_master_exn ~tls cluster_cfg =
  let open Result in
  find_master ~tls cluster_cfg >>=
    function
    | Ok r -> return r
    | Error e -> Lwt.fail (Master_lookup_error e)

let find_master_exn' ~tls cluster_cfg =
  find_master_exn ~tls cluster_cfg >>= fun master_cfg ->
  Lwt.return master_cfg.NodeConfig.name

let run f = Lwt_main.run (f ()); 0


let with_master_client ~tls cluster_cfg f =
  find_master_exn ~tls cluster_cfg >>= fun master_cfg ->
  with_client ~tls master_cfg cluster_cfg.ClusterClientConfig.id f

let run_master ~tls ccfg f =
  run (fun () -> with_master_client ~tls ccfg f)

let set ~tls ccfg key value =
  run_master ~tls ccfg (fun client -> client # set key value)

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

let who_master ~tls ccfg () =
  let t () =
    find_master_exn' ~tls ccfg >>= fun master_name ->
    Lwt_io.printl master_name
  in
  run t

let run_client ~tls node_cfg cluster_id f =
  run (fun () -> with_client ~tls node_cfg cluster_id f)

let node_state ~tls node_cfg cluster_id =
  run_client ~tls node_cfg cluster_id
             (fun client -> client # current_state () >>= fun state ->
                            Lwt_io.printl state)


let node_version ~tls node_cfg cluster_id =
  run_client ~tls node_cfg cluster_id
             (fun client ->
              client # version () >>= fun (major,minor,patch, info) ->
              Lwt_io.printlf "%i.%i.%i" major minor patch >>= fun () ->
              Lwt_io.printl info
             )
