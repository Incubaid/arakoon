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
open Routing
open Client_cfg
open Ncfg
open Interval
open Arakoon_client

let section = Logger.Section.main

let so2s = Log_extra.string_option2s

let try_connect (ips, port) =
  Lwt.catch
    (fun () ->
       let sa = Network.make_address ips port in
       Network.__open_connection sa >>= fun (ic,oc) ->
       let r = Some (ic,oc) in
       Lwt.return r
    )
    (function
      | Canceled -> Lwt.fail Canceled
      | exn -> Lwt.return None)




module NC = struct
  type connection = Lwt_io.input_channel * Lwt_io.output_channel
  type lc =
    | Address of (string list * int)
    | Connection of connection

  type nn = string * string (* cluster_name,node_name *)

  type t = {
    rc : NCFG.t;
    keeper_cn: string;
    connections : (nn ,lc) Hashtbl.t;
    masters: (string, string option) Hashtbl.t;
  }

  let make rc keeper_cn =
    let masters = Hashtbl.create 5 in
    let () = NCFG.iter_cfgs rc (fun k _ -> Hashtbl.add masters k None) in
    let connections = Hashtbl.create 13 in
    let () = NCFG.iter_cfgs rc
               (fun cluster v ->
                  Hashtbl.iter (fun node (ip,port) ->
                      let nn = (cluster,node) in
                      let a = Address (ip,port) in
                      Hashtbl.add connections nn a) v)
    in
    {rc; connections;masters;keeper_cn}

  let _get_connection t nn =
    let (cn,node) = nn in
    match Hashtbl.find t.connections nn with
      | Address (ips,port) ->
        begin
          let ip = List.hd ips in
          try_connect (ip,port) >>= function
          | Some conn ->
            Common.prologue cn conn >>= fun () ->
            let () = Hashtbl.add t.connections nn (Connection conn) in
            Lwt.return conn
          | None -> Llio.lwt_failfmt "Connection to (%s,%i) failed" ip port
        end
      | Connection conn -> Lwt.return conn

  let _find_master_remote t cn =
    let ccfg = NCFG.get_cluster t.rc cn in
    let node_names = ClientCfg.node_names ccfg in
    Logger.debug_f_ "names:%s" (Log_extra.list2s (fun s->s) node_names)
    >>= fun () ->
    Lwt_list.map_s
      (fun n ->
         let nn = (cn,n) in
         _get_connection t nn
      )
      node_names
    >>= fun connections ->
    let get_master acc conn =
      begin
        match acc with
          | None ->
            Common.who_master conn
          | x -> Lwt.return x
      end
    in
    Lwt_list.fold_left_s get_master None connections >>= function
    | None ->
      Logger.error_f_ "Could not find master for cluster %s" cn >>= fun () ->
      Lwt.fail (Failure "Could not find master.")
    | Some m ->
      Logger.debug_f_ "Found master %s" m >>= fun () ->
      Lwt.return m



  let _find_master t cn =
    let m = Hashtbl.find t.masters cn in
    match m with
      | None -> _find_master_remote t cn
      | Some master -> Lwt.return master

  let _with_master_connection t cn (todo: connection -> 'c Lwt.t) =
    Logger.debug_f_ "_with_master_connection %s" cn >>= fun () ->
    _find_master t cn >>= fun master ->
    let nn = cn, master in
    _get_connection t nn >>= fun connection ->
    todo connection

  let __migrate t clu_left sep clu_right publish migration =
    Logger.debug_f_ "migrate %s" (Log_extra.string_option2s sep) >>= fun () ->
    let from_cn, to_cn, direction = migration in
    Logger.debug_f_ "from:%s to:%s" from_cn to_cn >>= fun () ->
    let pinch () =
      Logger.debug_ "pinch" >>= fun () ->
      _with_master_connection t from_cn
                              (fun conn ->
                               Common.pinch_fringe conn direction sep)
    in
    let push_fringe left right kvs =
      Logger.debug_f_ "push %S %s" left (Log_extra.string_option2s right) >>= fun () ->
      _with_master_connection t to_cn
                              (fun conn ->
                               Common.accept_fringe conn left right kvs)
    in
    ignore (push_fringe "" None []);
    let delete_fringe left right =
      Logger.debug_f_ "delete_fringe %S %s" left (Log_extra.string_option2s right) >>= fun () ->
      _with_master_connection t from_cn
        (fun conn -> Common.remove_fringe conn left right)
    in

    (* TODO reuse master connection if possible? *)

    let cl_left, cl_right = match direction with
      | Routing.UPPER_BOUND -> from_cn, to_cn
      | Routing.LOWER_BOUND -> to_cn, from_cn
    in

    let rec loop () =
      pinch () >>= fun (kvs,(b_left,b_right)) ->
      if (Some b_left) = b_right
      then
        begin
          assert (kvs = []);
          publish b_right cl_left cl_right >>= fun () ->
          Logger.debug_f_ "Migration has finished"
        end
      else
        begin
          push_fringe b_left b_right kvs >>= fun () ->
          delete_fringe b_left b_right >>= fun () ->
          publish (match direction with
                   | Routing.UPPER_BOUND -> Some b_left
                   | Routing.LOWER_BOUND -> b_right)
                  cl_left
                  cl_right
          >>= fun () ->
          loop ()
        end
    in
    loop ()


  let migrate t clu_left (sep: string) clu_right  =
    let r = NCFG.get_routing t.rc in
    let from_cn, to_cn, direction = Routing.get_diff r clu_left sep clu_right in
    let publish sep left right =
      let route = NCFG.get_routing t.rc in
      Logger.debug_f_ "old_route:%S" (Routing.to_s route) >>= fun () ->
      Logger.debug_f_ "left: %s - sep: %s - right: %s" left (Log_extra.string_option2s sep) right >>= fun () ->
      begin
        match sep with
          | Some sep ->
            let new_route = Routing.change route left sep right in
            let () = NCFG.set_routing t.rc new_route in
            Logger.debug_f_ "new route:%S" (Routing.to_s new_route) >>= fun () ->
            _with_master_connection t t.keeper_cn
              (fun conn -> Common.set_routing_delta conn left sep right)
          | None -> failwith "Cannot end up with an empty separator during regular migration"
      end
    in
    __migrate t clu_left (Some sep) clu_right publish (from_cn, to_cn, direction)

  let delete t (cluster_id: string) (sep: string option) =
    Logger.debug_f_ "Nursery.delete %s %s" cluster_id (so2s sep) >>= fun () ->
    let r = NCFG.get_routing t.rc in
    let lower = Routing.get_lower_sep None cluster_id r in
    let upper = Routing.get_upper_sep None cluster_id r in
    let publish sep left right =
      let route = NCFG.get_routing t.rc in
      Logger.debug_f_ "old_route:%S" (Routing.to_s route) >>= fun () ->
      Logger.debug_f_ "left: %s - sep: %s - right: %s" left (Log_extra.string_option2s sep) right >>= fun () ->
      begin
        match sep with
          | Some sep ->
            let new_route = Routing.change route left sep right in
            let () = NCFG.set_routing t.rc new_route in
            Logger.debug_f_ "new route:%S" (Routing.to_s new_route) >>= fun () ->
            _with_master_connection t t.keeper_cn
              (fun conn -> Common.set_routing_delta conn left sep right)
          | None ->
            let new_route = Routing.remove route cluster_id in
            let () = NCFG.set_routing t.rc new_route in
            Logger.debug_f_ "new route:%S" (Routing.to_s new_route) >>= fun () ->
            _with_master_connection t t.keeper_cn
              (fun conn -> Common.set_routing conn new_route)
      end
    in
    begin
      Logger.debug_f_ "delete lower - upper : %s - %s" (so2s lower) (so2s upper) >>= fun () ->
      match lower, upper with
        | None, None -> failwith "Cannot remove last cluster from nursery"
        | Some x, None ->
          begin
            match sep with
              | None ->
                let m_prev = Routing.prev_cluster r cluster_id in
                begin
                  match m_prev with
                    | None -> failwith "Invalid routing request. No previous??"
                    | Some prev ->
                      __migrate t cluster_id sep prev
                        publish (cluster_id, prev, Routing.UPPER_BOUND)
                end
              | Some y ->
                failwith "Cannot set separator when removing a boundary cluster from the nursery"
          end
        | None, Some x ->
          begin
            match sep with
              | None ->
                let m_next = Routing.next_cluster r cluster_id in
                begin
                  match m_next with
                    | None -> failwith "Invalid routing request. No next??"
                    | Some next ->
                      __migrate t cluster_id sep next
                        publish (cluster_id, next, Routing.LOWER_BOUND)
                end
              | Some y ->
                failwith "Cannot set separator when removing a boundary cluster from a nursery"
          end
        | Some x, Some y ->
          begin
            match sep with
              | None ->
                failwith "Need to set replacement boundary when removing an inner cluster from the nursery"
              | Some y ->
                let m_next = Routing.next_cluster r cluster_id in
                let m_prev = Routing.prev_cluster r cluster_id in
                begin
                  match m_next, m_prev with
                    | Some next, Some prev ->
                      __migrate t cluster_id sep prev
                        publish (cluster_id, prev, Routing.UPPER_BOUND)
                      >>= fun () ->
                      __migrate t cluster_id sep next
                        publish (cluster_id, next, Routing.LOWER_BOUND)
                    | _ -> failwith "Invalid routing request. No next or previous??"
                end
          end
    end

end

(* let nursery_test_main () = *)
(*   All_test.configure_logging (); *)
(*   let repr = [("left", "ZZ")], "right" in (\* all in left *\) *)
(*   let routing = Routing.build repr in *)
(*   let left_cfg = ClientCfg.make () in *)
(*   let right_cfg = ClientCfg.make () in *)
(*   let () = ClientCfg.add left_cfg "left_0"   (["127.0.0.1"], 4000) in *)
(*   let () = ClientCfg.add right_cfg "right_0" (["127.0.0.1"], 5000) in *)
(*   let nursery_cfg = NCFG.make routing in *)
(*   let () = NCFG.add_cluster nursery_cfg "left" left_cfg in *)
(*   let () = NCFG.add_cluster nursery_cfg "right" right_cfg in *)
(*   let keeper = "left" in *)
(*   let nc = NC.make nursery_cfg keeper in *)
(*   (\* *)
(*   let test k v = *)
(*     NC.set client k v >>= fun () -> *)
(*     NC.get client k >>= fun v' -> *)
(*     Logger.debug_f_ "get '%s' yields %s" k v' >>= fun () -> *)
(*     Logger.debug_ "done" *)
(*   in *)
(*   let t () = *)
(*     test "a" "A" >>= fun () -> *)
(*     test "z" "Z" *)
(*   *\) *)
(*   let t () = *)
(*     Logger.info_ "pre-fill" >>= fun () -> *)
(*     let rec fill i = *)
(*       if i = 64 *)
(*       then Lwt.return () *)
(*       else *)
(*         let k = Printf.sprintf "%c" (Char.chr i) *)
(*         and v = Printf.sprintf "%c_value" (Char.chr i) in *)
(*         NC.set nc k v >>= fun () -> *)
(*         fill (i-1) *)
(*     in *)
(*     let left_i  = Interval.max    (\* all *\) *)
(*     and right_i = Interval.max in (\* all *\) *)
(*     NC.force_interval nc "left" left_i >>= fun () -> *)
(*     NC.force_interval nc "right" right_i >>= fun () -> *)
(*     fill 90 >>= fun () -> *)

(*     NC.migrate nc "left" "T" "right" *)
(*   in *)
(*   Lwt_main.run (t ()) *)
