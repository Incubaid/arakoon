(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
*)

open Node_cfg.Node_cfg
open Network
open Statistics
open Lwt
open Lwt_extra

let section = Logger.Section.main

exception No_connection

let with_connection' addrs f =
  let count = List.length addrs in
  let res = Lwt_mvar.create_empty () in
  let err = Lwt_mvar.create None in
  let l = Lwt_mutex.create () in
  let cd = CountDownLatch.create ~count in

  let f' addr =
    Lwt.catch
      (fun () ->
        Lwt_io.with_connection addr (fun c ->
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


let with_client node_cfg cluster_id f =
  let addrs = List.map (fun ip -> make_address ip node_cfg.client_port) node_cfg.ips in
  let do_it _addr connection =
    Arakoon_remote_client.make_remote_client cluster_id connection >>= f
  in
  with_connection' addrs do_it

let with_remote_nodestream node_cfg cluster_id f =
  let addrs = List.map (fun ip -> make_address ip node_cfg.client_port) node_cfg.ips in
  let do_it _addr connection =
    Remote_nodestream.make_remote_nodestream cluster_id connection >>= f
  in
  with_connection' addrs do_it

let ping ip port cluster_id =
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
  let t = Lwt_io.with_connection sa do_it in
  Lwt_main.run t; 0



let find_master cluster_cfg =
  let cfgs = cluster_cfg.cfgs in
  let rec loop = function
    | [] -> Lwt.fail (Failure "too many nodes down")
    | cfg :: rest ->
        begin
          let section = Logger.Section.main in
          Logger.info_f_ "cfg=%s" cfg.node_name >>= fun () ->
          Lwt.catch
            (fun () ->
              with_client
                cfg cluster_cfg.cluster_id
                (fun client ->
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

let with_master_client cfg_name f =
  let ccfg = read_config cfg_name in
  let cfgs = ccfg.cfgs in
  find_master ccfg >>= fun master_name ->
  let master_cfg = List.hd (List.filter (fun cfg -> cfg.node_name = master_name) cfgs) in
  with_client master_cfg ccfg.cluster_id f

let set cfg_name key value =
  let t () = with_master_client cfg_name (fun client -> client # set key value)
  in run t

let get cfg_name key =
  let f (client:Arakoon_client.client) =
    client # get key >>= fun value ->
    Lwt_io.printlf "%S%!" value
  in
  let t () = with_master_client cfg_name f in
  run t


let get_key_count cfg_name () =
  let f (client:Arakoon_client.client) =
    client # get_key_count () >>= fun c64 ->
    Lwt_io.printlf "%Li%!" c64
  in
  let t () = with_master_client cfg_name f in
  run t

let delete cfg_name key =
  let t () = with_master_client cfg_name (fun client -> client # delete key )
  in
  run t

let delete_prefix cfg_name prefix =
  let t () = with_master_client cfg_name (
    fun client -> client # delete_prefix prefix >>= fun n_deleted ->
      Lwt_io.printlf "%i" n_deleted
  )
  in
  run t

let prefix cfg_name prefix prefix_size =
  let t () = with_master_client cfg_name
    (fun client ->
      client # prefix_keys prefix prefix_size >>= fun keys ->
      Lwt_list.iter_s (fun k -> Lwt_io.printlf "%S" k ) keys >>= fun () ->
      Lwt.return ()
    )
  in
  run t

let range_entries cfg_name left linc right rinc max_results =
  let t () =
    with_master_client
      cfg_name
      (fun client ->
        client # range_entries ~allow_dirty:false ~first:left ~finc:linc ~last:right ~linc:rinc ~max:max_results >>= fun entries ->
       Lwt_list.iter_s (fun (k,v) -> Lwt_io.printlf "%S %S" k v ) entries >>= fun () ->
       let size = List.length entries in
       Lwt_io.printlf "%i listed" size >>= fun () ->
       Lwt.return ()
      )
  in
  run t

let rev_range_entries cfg_name left linc right rinc max_results =
  let t () =
    with_master_client
      cfg_name
      (fun client ->
       client # rev_range_entries ~allow_dirty:false ~first:left ~finc:linc ~last:right ~linc:rinc ~max:max_results >>= fun entries ->
       Lwt_list.iter_s (fun (k,v) -> Lwt_io.printlf "%S %S" k v ) entries >>= fun () ->
       let size = List.length entries in
       Lwt_io.printlf "%i listed" size >>= fun () ->
       Lwt.return ()
      )
  in
  run t

let benchmark cfg_name size tx_size max_n n_clients =
  Lwt_io.set_default_buffer_size 32768;
  let t () =
    let with_c = with_master_client cfg_name in
    Benchmark.benchmark ~with_c ~size ~tx_size ~max_n n_clients
  in
  run t


let expect_progress_possible cfg_name =
  let f client =
    client # expect_progress_possible () >>= fun b ->
    Lwt_io.printlf "%b" b
  in
  let t () = with_master_client cfg_name f
  in
  run t


let statistics cfg_name =
  let f client =
    client # statistics () >>= fun statistics ->
    let rep = Statistics.string_of statistics in
    Lwt_io.printl rep
  in
  let t () = with_master_client cfg_name f
  in run t

let who_master cfg_name () =
  let cluster_cfg = read_config cfg_name in
  let t () =
    find_master cluster_cfg >>= fun master_name ->
    Lwt_io.printl master_name
  in
  run t

let _cluster_and_node_cfg node_name cfg_name =
 let cluster_cfg = read_config cfg_name in
  let find cfgs =
    let rec loop = function
      | [] -> failwith (node_name ^ " is not known in config " ^ cfg_name)
      | cfg :: rest ->
        if cfg.node_name = node_name then cfg
        else loop rest
    in
    loop cfgs
  in
  let node_cfg = find cluster_cfg.cfgs in
  cluster_cfg, node_cfg

let node_state node_name cfg_name =
  let cluster_cfg,node_cfg = _cluster_and_node_cfg node_name cfg_name in
  let cluster = cluster_cfg.cluster_id in
  let f client =
    client # current_state () >>= fun state ->
    Lwt_io.printl state
  in
  let t () = with_client node_cfg cluster f in
  run t



let node_version node_name cfg_name =
  let cluster_cfg, node_cfg = _cluster_and_node_cfg node_name cfg_name in
  let cluster = cluster_cfg.cluster_id in
  let t () =
    with_client node_cfg cluster
      (fun client ->
        client # version () >>= fun (major,minor,patch, info) ->
        Lwt_io.printlf "%i.%i.%i" major minor patch >>= fun () ->
        Lwt_io.printl info
      )
  in
  run t
