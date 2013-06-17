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

let _address_to_use ips port = make_address (List.hd ips) port 

let with_client cfg (cluster:string) f =
  let sa = _address_to_use cfg.ips cfg.client_port in
  let do_it connection =
    Arakoon_remote_client.make_remote_client cluster connection 
    >>= fun (client: Arakoon_client.client) ->
    f client
  in
    Lwt_io.with_connection sa do_it

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
  Lwt_extra.run t; 0



let find_master cluster_cfg =
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
              Lwt_io.with_connection sa
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

let run f = Lwt_extra.run (f ()); 0

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


let node_version node_name cfg_name = 
  let cluster_cfg = read_config cfg_name in
  let rec _find cfgs = 
    let rec loop = function
      | [] -> failwith (node_name ^ " is not known in config " ^ cfg_name) 
      | cfg :: rest ->
        if cfg.node_name = node_name then cfg
        else loop rest
    in
    loop cfgs
  in
  let node_cfg = _find cluster_cfg.cfgs in
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
