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

open Network
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


let get_tls_from_ssl_cfg (ssl_cfg : Arakoon_client_config.ssl_cfg option) =
  let open Arakoon_client_config in
  match ssl_cfg with
  | None -> None
  | Some ssl ->
     let ca_cert = ssl.ca_cert in
     let protocol = ssl.protocol in
     let creds = ssl.creds in
     let ctx = default_create_client_context ~ca_cert ~creds ~protocol in
     Some ctx


let with_connection ~tls ~tcp_keepalive sa do_it =
  let fd = Lwt_unix.socket (Unix.domain_of_sockaddr sa) Unix.SOCK_STREAM 0 in
  Lwt.catch
    (fun () ->
      Tcp_keepalive.apply fd tcp_keepalive;
      Lwt_unix.setsockopt fd Lwt_unix.TCP_NODELAY true;
      Lwt_unix.set_close_on_exec fd;
      Lwt_unix.connect fd sa)
    (fun exn ->
      Lwt.catch
        (fun () -> Lwt_unix.close fd)
        (fun _ -> Lwt.return_unit)
      >>= fun () ->
      Lwt.fail exn
    )
  >>= fun () ->
  begin
  match tls with
  | None ->
     let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd
     and oc = Lwt_io.of_fd ~mode:Lwt_io.output fd in
     let conn = (ic,oc)
     and fin = fun () -> Lwt_unix.close fd
     in
     Lwt.return (conn,fin)
  | Some ctx ->
     Typed_ssl.Lwt.ssl_connect fd ctx >>= fun (_, sock) ->
     let ic = Lwt_ssl.in_channel_of_descr sock
     and oc = Lwt_ssl.out_channel_of_descr sock in
     let conn = (ic,oc)
     and fin = fun () -> Lwt_ssl.close sock
     in
     Lwt.return (conn,fin)
  end
  >>= fun (conn,fin) ->
  Lwt.finalize
    (fun () -> do_it conn)
    fin

exception No_connection

let with_connection' ~tls ~tcp_keepalive addrs f =
  let count = List.length addrs in
  let res = Lwt_mvar.create_empty () in
  let err = Lwt_mvar.create None in
  let l = Lwt_mutex.create () in
  let cd = CountDownLatch.create ~count in

  let f' addr =
    Lwt.catch
      (fun () ->
        with_connection ~tls ~tcp_keepalive addr (fun c ->
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


let with_client'
      ~tls
      (node_cfg : Arakoon_client_config.node_cfg)
      cluster_id
      f =
  let open Arakoon_client_config in
  let addrs = List.map (fun ip -> make_address ip node_cfg.port) node_cfg.ips in
  let do_it _addr connection =
    Arakoon_remote_client.make_remote_client cluster_id connection >>= f
  in
  with_connection' ~tls addrs do_it

(** Result type and utilities for {! find_master' } *)
module MasterLookupResult = struct

  (** Result type for a master lookup using {! find_master' } *)
  type t =
    | Found of string * Arakoon_client_config.node_cfg (** Master found *)
    | No_master
    | Too_many_nodes_down
    | Unknown_node of (string * (string * Arakoon_client_config.node_cfg)) (** An unknown node name was returned by a node *)
    | Exception of exn (** An exception occurred during lookup *)

  (** Create a {! string } representation of a {! MasterLookupResult.t } *)
  let to_string t =
    let open Arakoon_client_config in
    let open To_string in
    match t with
      | Found (name, cfg) -> Printf.sprintf "Found %s (%s)" name (show_node_cfg cfg)
      | No_master -> "No_master"
      | Too_many_nodes_down -> "Too_many_nodes_down"
      | Unknown_node (n, (n', cfg')) ->
        Printf.sprintf "Unknown_node (%S found on %s(%s))" n n' (show_node_cfg cfg')
      | Exception exn -> Printf.sprintf "Exception (%s)" (Printexc.to_string exn)

  exception Error of t
end

(** Lookup a master node in a cluster

    The result is wrapped in a {! MasterLookupResult.t }, including (if
    applicable) exceptions, except {! Lwt.Canceled } which is passed through
    (you most likely don't want to catch that anyway).
*)
let find_master' ?tls cluster_cfg =
  let open Arakoon_client_config in

  let tls = match tls with
    | None -> get_tls_from_ssl_cfg cluster_cfg.ssl_cfg
    | Some x -> Some x
  in

  let lookup_cfg n =
    let rec loop = function
      | [] -> None
      | ((node_name, cfg) :: xs) -> begin
          if node_name = n
            then Some cfg
            else loop xs
      end
    in
    loop (cluster_cfg.node_cfgs)
  in

  let open MasterLookupResult in

  let rec loop unknown = function
    | [] -> begin
        let res =
          if unknown = []
            then
              Too_many_nodes_down
            else
              No_master
        in
        Logger.debug_f_
          "Client_main.find_master': %s" (MasterLookupResult.to_string res) >>= fun () ->
        Lwt.return res
    end
    | ((node_name, cfg) :: rest) -> begin
        Logger.debug_f_ "Client_main.find_master': Trying %S" node_name >>= fun () ->
        Lwt.catch
          (fun () ->
           Lwt.choose
             [ (Lwt_unix.sleep 1. >>= fun () ->
                Lwt.return None);
               with_client'
                 ~tls
                 ~tcp_keepalive:cluster_cfg.tcp_keepalive
                 cfg cluster_cfg.cluster_id
                 (fun client ->
                  client # who_master ()); ]
           >>= function
                | None -> begin
                    Logger.debug_f_
                      "Client_main.find_master': %S doesn't know" node_name >>= fun () ->
                    loop ((node_name, cfg) :: unknown) rest
                end
                | Some n -> begin
                    Logger.debug_f_ "Client_main.find_master': %S thinks %S" node_name n >>= fun () ->
                    if n = node_name
                      then begin
                        let res = Found (node_name, cfg) in
                        Logger.debug_f_ "Client_main.find_master': %s" (to_string res) >>= fun () ->
                        return res
                      end
                      else
                        match lookup_cfg n with
                          | Some ncfg ->
                              loop unknown ((n, ncfg) :: rest)
                          | None -> begin
                              let res = Unknown_node (n, (node_name, cfg)) in
                              Logger.warning_f_
                                "Client_main.find_master': %s" (to_string res) >>= fun () ->
                              return res
                          end
                end)
          (function
           | Lwt.Canceled as exn ->
              Lwt.fail exn
           | exn ->
              Logger.debug_f_ ~exn
                "Client_main.find_master': problem with %S" node_name >>= fun () ->
              loop unknown rest
          )
    end
  in
  loop [] (cluster_cfg.node_cfgs)


(** Lookup the master of a cluster in a loop

    This action calls {! find_master' } in a loop, as long as it returns
    {! MasterLookupResult.No_master } or
    {! MasterLookupResult.Too_many_nodes_down }. Other return values are passed
    through to the caller.

    In some circumstances, this action could loop forever, so it's mostly
    useful in combination with {! Lwt_unix.with_timeout } or something related.
*)
let find_master_loop cluster_cfg =
  let open MasterLookupResult in
  let rec loop () =
    find_master' cluster_cfg >>= fun r -> match r with
      | No_master
      | Too_many_nodes_down -> begin
          Logger.debug_f_ "Client_main.find_master_loop: %s" (to_string r) >>=
          loop
        end
      | Found _
      | Unknown_node _
      | Exception _ -> return r
  in
  loop ()

let with_client'' ?tls ccfg node_name f =
  let open Arakoon_client_config in
  with_client'
    ~tls:(match tls with
          | None -> get_tls_from_ssl_cfg ccfg.ssl_cfg
          | Some x -> Some x)
    ~tcp_keepalive:ccfg.tcp_keepalive
    (get_node_cfg ccfg node_name)
    ccfg.cluster_id
    f

let with_master_client' cluster_cfg f =
  let open MasterLookupResult in
  find_master' cluster_cfg >>= function
  | Found (master_name, master_cfg) ->
     with_client'' cluster_cfg master_name f
  | master_result ->
    Lwt.fail (Error master_result)
