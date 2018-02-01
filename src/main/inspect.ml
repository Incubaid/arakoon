(*
Copyright (2018) iNuron BVBA

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

open Lwt.Infix
open Node_cfg

module S = (val (Store.make_store_module (module Batched_store.Local_store)))

let ensure_dir d p =
  Lwt.catch
    (fun () -> File_system.mkdir d p)
    (function | Unix.Unix_error (Unix.EEXIST, _, _) -> Lwt.return ())

let inspect_cluster ~tls (cfg : Node_cfg.cluster_cfg) =

  let cluster_id = cfg.cluster_id in
  ensure_dir cluster_id 0o755 >>= fun () ->
  Lwt_list.map_p
    (fun node_cfg ->
      let open Node_cfg in
      let node_name = node_cfg.node_name in
      let path = cluster_id ^ "/" ^ node_name in
      ensure_dir path 0o755 >>= fun () ->

      (* TODO auto handle closing of tlog... *)
      Tlc2.make_tlc2
        ~cluster_id:cfg.cluster_id
        ~compressor:Compression.Snappy
        path path path
        ~fsync:false node_name ~fsync_tlog_dir:false
      >>= fun tlc ->

      S.make_store ~lcnum:1024 ~ncnum:512 (path ^ "/store.db") ~cluster_id
      >>= fun store ->

      Catchup.catchup_tlog
        ~tls_ctx:tls
        ~tcp_keepalive:Tcp_keepalive.default_tcp_keepalive
        ~stop:(ref false)
        cfg.cfgs
        ~cluster_id
        node_name
        ((module S), store, tlc) >>= fun tlog_i ->

      Lwt_log.info_f "Got head & tlogs for %s, i=%Li" node_name tlog_i >>= fun () ->
      Lwt.return (node_name, store, tlc, tlog_i)
    )
    cfg.cfgs
  >>= fun nodes ->

  let minimum_last_i =
    List.fold_left
      (fun minimum (_, _, _, current) -> min minimum current)
      Int64.max_int
      nodes
  in

  Lwt_log.info_f "Got all heads & tlogs, minimal i=%Li" minimum_last_i >>= fun () ->

  Lwt_list.iter_s
    (fun (node_name, store, tlc, _) ->
      Catchup.catchup_store
        ~stop:(ref false)
        node_name
        ((module S), store, tlc)
        minimum_last_i >>= fun () ->
      S.close store >>= fun () ->

      tlc # close ()
    )
    nodes >>= fun () ->

  (* - diff already, tell which are same
   * - show diff cmds
   *)

  Lwt.return ()
