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


open Std
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
        ~should_fsync:(fun _ _ -> false)
        ~fsync_tlog_dir:false node_name
      >>= fun tlc ->

      let db_path = path ^ "/store.db" in
      S.make_store ~lcnum:1024 ~ncnum:512 db_path ~cluster_id
      >>= fun store ->

      Catchup.catchup_tlog
        ~tls_ctx:tls
        ~tcp_keepalive:Tcp_keepalive.default_tcp_keepalive
        ~stop:(ref false)
        cfg.cfgs
        ~cluster_id
        node_name
        ((module S), store, tlc) >>= fun tlog_i ->

      let tlog_i = Option.get_some tlog_i in

      Lwt_log.info_f "Got head & tlogs for %s, i=%Li" node_name tlog_i >>= fun () ->
      Lwt.return (node_name, db_path, store, tlc, tlog_i)
    )
    cfg.cfgs
  >>= fun nodes ->

  let minimum_last_i =
    List.fold_left
      (fun minimum (_, _, _, _, current) -> min minimum current)
      Int64.max_int
      nodes
  in

  Lwt_log.info_f "Got all heads & tlogs, minimal i=%Li" minimum_last_i >>= fun () ->

  Lwt_list.iter_s
    (fun (node_name, db_path, store, tlc, _) ->
      Catchup.catchup_store
        ~stop:(ref false)
        node_name
        ((module S), store, tlc)
        minimum_last_i >>= fun () ->
      S.close store >>= fun () ->

      tlc # close () >>= fun () ->

      let read_only = true in
      Local_store.make_store ~lcnum:1024 ~ncnum:512 read_only db_path
      >>= fun store ->

      File_system.with_fd
        (db_path ^ ".dump")
        ~flags:[ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC; Unix.O_NONBLOCK; Unix.O_EXCL; ]
        ~perm:0o644
        (fun fd_out ->
          let ch_out = Lwt_io.of_fd ~mode:Lwt_io.output fd_out in
          let open Camltc in
          Hotc.with_cursor
            (Hotc.get_bdb store.Local_store.db)
            (fun bdb cursor ->
              let () = Bdb.first bdb cursor in
              let rec loop () =
                let key = Bdb.key bdb cursor in
                let value = Bdb.value bdb cursor in
                Lwt_io.fprintlf ch_out "key=%S value=%S" key value >>= fun () ->
                let continue =
                  try Bdb.next bdb cursor; true
                  with Not_found -> false
                in
                if continue
                then loop ()
                else Lwt_io.fprintlf ch_out "Finished iterating over db"
              in
              loop ()
            ) >>= fun () ->
          Lwt_io.flush ch_out
        )
    )
    nodes >>= fun () ->

  Lwt_io.printlf "\nnow run the following commands to inspect the differences (if any)" >>= fun () ->
  let _, first, _, _, _ = List.hd nodes in
  Lwt_list.iter_s
    (fun (_, db_path, _, _, _) ->
      Lwt_io.printlf "diff %s.dump %s.dump" first db_path)
    (List.tl nodes)
