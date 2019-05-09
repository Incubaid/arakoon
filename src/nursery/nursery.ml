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
open Arakoon_interval
open Arakoon_client

let section = Logger.Section.main

let so2s = Log_extra.string_option2s

let try_connect ~tcp_keepalive (ips, port) =
  Lwt.catch
    (fun () ->
       let sa = Network.make_address ips port in
       Network.__open_connection ~tcp_keepalive sa >>= fun (_, ic, oc) ->
       let r = Some (ic,oc) in
       Lwt.return r
    )
    (function
      | Canceled -> Lwt.fail Canceled
      | _ -> Lwt.return None)


module NC = struct
  type connection = Lwt_io.input_channel * Lwt_io.output_channel
  type lc =
    | Address of (string list * int)
    | Connection of connection

  type nn = string * string (* cluster_name,node_name *)

  type t = {
    rc : NCFG.t;
    tcp_keepalive : Tcp_keepalive.t;
    keeper_cn: string;
    connections : (nn ,lc) Hashtbl.t;
    masters: (string, string option) Hashtbl.t;
  }

  let make rc keeper_cn tcp_keepalive =
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
    { rc; connections; masters; keeper_cn; tcp_keepalive; }

  let _get_connection t nn =
    let (cn,_node) = nn in
    match Hashtbl.find t.connections nn with
      | Address (ips,port) ->
        begin
          let ip = List.hd ips in
          try_connect ~tcp_keepalive:t.tcp_keepalive (ip,port) >>= function
          | Some conn ->
             Lwt.catch
               (fun () ->
                Protocol_common.prologue cn conn >>= fun () ->
                let () = Hashtbl.add t.connections nn (Connection conn) in
                Lwt.return conn)
               (fun exn ->
                let ic, oc = conn in
                Lwt_io.close ic >>= fun () ->
                Lwt_io.close oc >>= fun () ->
                Lwt.fail exn)
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
            Protocol_common.who_master conn
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

  let set t key value =
    let cn = NCFG.find_cluster t.rc key in
    Logger.debug_f_ "set %S => %s" key cn >>= fun () ->
    let todo conn = Protocol_common.set conn key value in
    _with_master_connection t cn todo

  let get t key =
    let cn = NCFG.find_cluster t.rc key in
    let todo conn = Protocol_common.get conn ~consistency:Consistent key in
    _with_master_connection t cn todo

  let force_interval t cn i =
    Logger.debug_f_ "force_interval %s: %s" cn (Interval.to_string i) >>= fun () ->
    _with_master_connection t cn
      (fun conn -> Protocol_common.set_interval conn i)


  let close _t = Llio.lwt_failfmt "close not implemented"

  let __migrate t _clu_left sep _clu_right finalize publish migration =
    Logger.debug_f_ "migrate %s" (Log_extra.string_option2s sep) >>= fun () ->
    let from_cn, to_cn, direction = migration in
    Logger.debug_f_ "from:%s to:%s" from_cn to_cn >>= fun () ->
    let pull () =
      Logger.debug_ "pull">>= fun () ->
      _with_master_connection t from_cn
        (fun conn -> Protocol_common.get_fringe conn sep direction )
    in
    let push fringe i =
      let seq = List.map (fun (k,v) -> Arakoon_client.Set(k,v)) fringe in
      Logger.debug_ "push" >>= fun () ->
      _with_master_connection t to_cn
        (fun conn -> Protocol_common.migrate_range conn i seq)
    in
    let delete fringe =
      let seq = List.map (fun (k,_v) -> Arakoon_client.Delete k) fringe in
      Logger.debug_ "delete" >>= fun () ->
      _with_master_connection t from_cn
        (fun conn -> Protocol_common.sequence conn seq)
    in
    let get_next_key k =
      k ^ (String.make 1 (Char.chr 1))
    in
    let get_interval cn = _with_master_connection t cn Protocol_common.get_interval in
    let set_interval cn i = force_interval t cn i in
    let i2s i = Interval.to_string i in
    Logger.debug_f_ "Getting initial interval from %s" from_cn >>= fun () ->
    get_interval from_cn >>= fun from_i ->
    Logger.debug_f_ "Getting initial interval from %s" to_cn >>= fun () ->
    get_interval to_cn >>= fun to_i ->
    let open Arakoon_interval in
    let rec loop (from_i:Interval.t) (to_i:Interval.t) =
      pull () >>= fun fringe ->
      match fringe with
        | [] ->
          begin
            finalize from_i to_i
          end
        | fringe ->
          let size = List.length fringe in
          Logger.debug_f_ "Length = %i" size >>= fun () ->
          (*
           - change public interval on 'from'
           - push fringe & change private interval on 'to'
           - delete fringe & change private interval on 'from'
           - change public interval 'to'
           - publish new route.

          *)
          let fpu_b = from_i.pu_b
          and fpu_e = from_i.pu_e
          and fpr_b = from_i.pr_b
          and fpr_e = from_i.pr_e
          in
          let tpu_b = to_i.pu_b
          and tpu_e = to_i.pu_e
          and tpr_b = to_i.pr_b
          and tpr_e = to_i.pr_e
          in
          begin
            match direction with
              | Routing.UPPER_BOUND ->
                let b, _ = List.hd fringe in
                let e, _ = List.hd( List.rev fringe ) in
                let e = get_next_key  e in
                Logger.debug_f_ "b:%S e:%S" b e >>= fun () ->
                let from_i' = Interval.make (Some e) fpu_e fpr_b fpr_e in
                set_interval from_cn from_i' >>= fun () ->
                let to_i1 = Interval.make tpu_b tpu_e tpr_b (Some e) in
                push fringe to_i1 >>= fun () ->
                delete fringe >>= fun () ->
                Logger.debug_ "Fringe now is deleted. Time to change intervals" >>= fun () ->
                let to_i2 = Interval.make tpu_b (Some e) tpr_b (Some e) in
                set_interval to_cn to_i2 >>= fun () ->
                let from_i2 = Interval.make (Some e) fpu_e (Some e) fpr_e in
                set_interval from_cn from_i2 >>= fun () ->
                Lwt.return (e, from_i2, to_i2, to_cn, from_cn)

              | Routing.LOWER_BOUND ->
                let b, _ = List.hd( List.rev fringe ) in
                let e, _ = List.hd fringe in
                Logger.debug_f_ "b:%S e:%S" b e >>= fun () ->
                let from_i' = Interval.make fpu_b (Some b) fpr_b fpr_e in
                set_interval from_cn from_i' >>= fun () ->
                let to_i1 = Interval.make tpu_b tpu_e (Some b) tpr_e in
                push fringe to_i1 >>= fun () ->
                delete fringe >>= fun () ->
                Logger.debug_ "Fringe now is deleted. Time to change intervals" >>= fun () ->
                let to_i2 = Interval.make (Some b) tpu_e (Some b) tpr_e in
                set_interval to_cn to_i2 >>= fun () ->
                let from_i2 = Interval.make fpu_b (Some b) fpr_b (Some b) in
                set_interval from_cn from_i2 >>= fun () ->
                Lwt.return (b, from_i2, to_i2, from_cn, to_cn)
          end
          >>= fun (pub, from_i2, to_i2, left, right) ->
          (* set_interval to_cn to_i1 >>= fun () -> *)
          Logger.debug_f_ "from {%s:%s;%s:%s}" from_cn (i2s from_i) to_cn (i2s to_i) >>= fun () ->
          Logger.debug_f_ "to   {%s:%s;%s:%s}" from_cn (i2s from_i2) to_cn (i2s to_i2) >>= fun () ->
          publish (Some pub) left right >>= fun () ->
          loop from_i2 to_i2
    in
    loop from_i to_i


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
              (fun conn -> Protocol_common.set_routing_delta conn left sep right)
          | None -> failwith "Cannot end up with an empty separator during regular migration"
      end
    in
    let set_interval cn i = force_interval t (cn: string) (i: Interval.t) in
    let open Arakoon_interval in
    let finalize (from_i: Interval.t) (to_i: Interval.t) =
      Logger.debug_ "Setting final intervals en routing" >>= fun () ->

      let fpu_b = from_i.pu_b
      and fpu_e = from_i.pu_e
      and fpr_b = from_i.pr_b
      and fpr_e = from_i.pr_e
      in
      let tpu_b = to_i.pu_b
      and tpu_e = to_i.pu_e
      and tpr_b = to_i.pr_b
      and tpr_e = to_i.pr_e
      in
      let (from_i', to_i', left, right) =
        begin
          match direction with
            | Routing.UPPER_BOUND ->
              let from_i' = Interval.make (Some sep) fpu_e (Some sep) fpr_e in
              let to_i'   = Interval.make tpu_b (Some sep) tpr_b (Some sep) in
              from_i', to_i', to_cn, from_cn

            | Routing.LOWER_BOUND ->
              let from_i' = Interval.make fpu_b (Some sep) fpr_b (Some sep) in
              let to_i'   = Interval.make (Some sep) tpu_e (Some sep) tpr_e in
              from_i', to_i', from_cn, to_cn
        end
      in
      Logger.debug_f_ "final interval: from: %s" (Interval.to_string from_i') >>= fun () ->
      Logger.debug_f_ "final interval: to  : %s" (Interval.to_string to_i') >>= fun () ->
      set_interval from_cn from_i' >>= fun () ->
      set_interval to_cn to_i' >>= fun () ->
      publish (Some sep) left right
    in
    __migrate t clu_left (Some sep) clu_right finalize publish (from_cn, to_cn, direction)

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
              (fun conn -> Protocol_common.set_routing_delta conn left sep right)
          | None ->
            let new_route = Routing.remove route cluster_id in
            let () = NCFG.set_routing t.rc new_route in
            Logger.debug_f_ "new route:%S" (Routing.to_s new_route) >>= fun () ->
            _with_master_connection t t.keeper_cn
              (fun conn -> Protocol_common.set_routing conn new_route)
      end
    in
    let set_interval cn i = force_interval t (cn: string) (i: Interval.t) in
    let open Arakoon_interval in
    let finalize from_cn to_cn direction (from_i: Interval.t) (to_i: Interval.t) =
      Logger.debug_ "Setting final intervals en routing" >>= fun () ->
      let fpu_b = from_i.pu_b
      and fpu_e = from_i.pu_e
      and fpr_b = from_i.pr_b
      and fpr_e = from_i.pr_e
      in
      let tpu_b = to_i.pu_b
      and tpu_e = to_i.pu_e
      and tpr_b = to_i.pr_b
      and tpr_e = to_i.pr_e
      in
      let (from_i', to_i', left, right) =
        begin
          match direction with
            | Routing.UPPER_BOUND ->
              let from_i' = Interval.make sep fpu_e sep fpr_e in
              let to_i' = Interval.make tpu_b sep  tpr_b sep  in
              from_i', to_i', to_cn, from_cn

            | Routing.LOWER_BOUND ->
              let from_i' = Interval.make fpu_b sep fpr_b sep in
              let to_i' = Interval.make sep tpu_e sep tpr_e   in
              from_i', to_i', from_cn, to_cn
        end
      in
      Logger.debug_f_ "final interval: from: %s" (Interval.to_string from_i') >>= fun () ->
      Logger.debug_f_ "final interval: to  : %s" (Interval.to_string to_i') >>= fun () ->
      set_interval from_cn from_i' >>= fun () ->
      set_interval to_cn to_i' >>= fun () ->
      publish sep left right
    in
    begin
      Logger.debug_f_ "delete lower - upper : %s - %s" (so2s lower) (so2s upper) >>= fun () ->
      match lower, upper with
        | None, None -> failwith "Cannot remove last cluster from nursery"
        | Some _, None ->
          begin
            match sep with
              | None ->
                let m_prev = Routing.prev_cluster r cluster_id in
                begin
                  match m_prev with
                    | None -> failwith "Invalid routing request. No previous??"
                    | Some prev ->
                      __migrate t cluster_id sep prev
                        (finalize cluster_id prev Routing.UPPER_BOUND)
                        publish (cluster_id, prev, Routing.UPPER_BOUND)
                end
              | Some _ ->
                failwith "Cannot set separator when removing a boundary cluster from the nursery"
          end
        | None, Some _ ->
          begin
            match sep with
              | None ->
                let m_next = Routing.next_cluster r cluster_id in
                begin
                  match m_next with
                    | None -> failwith "Invalid routing request. No next??"
                    | Some next ->
                      __migrate t cluster_id sep next
                        (finalize cluster_id next Routing.LOWER_BOUND)
                        publish (cluster_id, next, Routing.LOWER_BOUND)
                end
              | Some _y ->
                failwith "Cannot set separator when removing a boundary cluster from a nursery"
          end
        | Some _x, Some _y ->
          begin
            match sep with
              | None ->
                failwith "Need to set replacement boundary when removing an inner cluster from the nursery"
              | Some _y ->
                let m_next = Routing.next_cluster r cluster_id in
                let m_prev = Routing.prev_cluster r cluster_id in
                begin
                  match m_next, m_prev with
                    | Some next, Some prev ->
                      __migrate t cluster_id sep prev
                        (finalize cluster_id prev Routing.UPPER_BOUND)
                        publish (cluster_id, prev, Routing.UPPER_BOUND)
                      >>= fun () ->
                      __migrate t cluster_id sep next
                        (finalize cluster_id next Routing.LOWER_BOUND)
                        publish (cluster_id, next, Routing.LOWER_BOUND)
                    | _ -> failwith "Invalid routing request. No next or previous??"
                end
          end
    end

end

let nursery_test_main () =
  All_test.configure_logging ();
  let repr = [("left", "ZZ")], "right" in (* all in left *)
  let routing = Routing.build repr in
  let left_cfg = ClientCfg.make () in
  let right_cfg = ClientCfg.make () in
  let () = ClientCfg.add left_cfg "left_0"   (["127.0.0.1"], 4000) in
  let () = ClientCfg.add right_cfg "right_0" (["127.0.0.1"], 5000) in
  let nursery_cfg = NCFG.make routing in
  let () = NCFG.add_cluster nursery_cfg "left" left_cfg in
  let () = NCFG.add_cluster nursery_cfg "right" right_cfg in
  let keeper = "left" in
  let nc = NC.make nursery_cfg keeper Tcp_keepalive.default_tcp_keepalive in
  (*
  let test k v =
    NC.set client k v >>= fun () ->
    NC.get client k >>= fun v' ->
    Logger.debug_f_ "get '%s' yields %s" k v' >>= fun () ->
    Logger.debug_ "done"
  in
  let t () =
    test "a" "A" >>= fun () ->
    test "z" "Z"
  *)
  let t () =
    Logger.info_ "pre-fill" >>= fun () ->
    let rec fill i =
      if i = 64
      then Lwt.return ()
      else
        let k = Printf.sprintf "%c" (Char.chr i)
        and v = Printf.sprintf "%c_value" (Char.chr i) in
        NC.set nc k v >>= fun () ->
        fill (i-1)
    in
    let left_i  = Interval.max    (* all *)
    and right_i = Interval.max in (* all *)
    NC.force_interval nc "left" left_i >>= fun () ->
    NC.force_interval nc "right" right_i >>= fun () ->
    fill 90 >>= fun () ->

    NC.migrate nc "left" "T" "right"
  in
  Lwt_main.run (t ())
