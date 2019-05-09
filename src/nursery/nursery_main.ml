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
open Node_cfg
open Routing
open Client_cfg

type lookup = (string, ClientCfg.node_address) Hashtbl.t

let with_remote_stream (cluster:string) cfg f =
  let ips,port = cfg in
  let ip0 = List.hd ips in
  let sa = Network.make_address ip0 port in
  let do_it connection =
    Remote_nodestream.make_remote_nodestream cluster connection
    >>= fun (client) ->
    f client
  in
  Lwt_io.with_connection sa do_it

let find_master cluster_id (cli_cfg:lookup) =
  let check_node node_name (node_cfg:ClientCfg.node_address) acc =
    begin
      Logger.info_f_ "node=%s" node_name >>= fun () ->
      let (ips,port) = node_cfg in
      let ip0 = List.hd ips in
      let sa = Network.make_address ip0 port in
      Lwt.catch
        (fun () ->
           Lwt_io.with_connection sa
             (fun connection ->
                Arakoon_remote_client.make_remote_client cluster_id  connection
                >>= fun client ->
                client # who_master ())
           >>= function
           | None -> acc
           | Some m -> Logger.info_f_ "master=%s" m >>= fun () ->
             Lwt.return (Some m) )
        (function
          | Unix.Unix_error(Unix.ECONNREFUSED,_,_ ) ->
            Logger.info_f_ "node %s is down, trying others" node_name >>= fun () ->
            acc
          | exn -> Lwt.fail exn
        )
    end
  in
  Hashtbl.fold check_node (cli_cfg:lookup) (Lwt.return None) >>= function
  | None -> failwith "No master found"
  | Some m -> Lwt.return m

let with_master_remote_stream cluster_id (cfg:lookup) f =
  find_master cluster_id cfg >>= fun master_name ->
  let master_cfg = Hashtbl.find cfg master_name in
  with_remote_stream cluster_id master_cfg f

let setup_logger file_name =
  Logger.Section.set_level Logger.Section.main Logger.Debug;
  Lwt_log.file
    ~template:"$(date): $(level): $(message)"
    ~mode:`Append ~file_name () >>= fun file_logger ->
  Lwt_log.default := file_logger;
  Lwt.return ()

let _get_keeper_config_generic fetcher _url=
  fetcher() >>= fun txt ->
  let inifile = new Arakoon_inifiles.inifile txt in
  let m_cfg = Node_cfg.get_nursery_cfg inifile in
  begin
    match m_cfg with
      | None -> failwith "No nursery keeper specified in config file"
      | Some (keeper_id, cli_cfg) ->
        Lwt.return (keeper_id, cli_cfg)
  end

let get_keeper_config cfg_url =
  let fetcher () = Arakoon_config_url.retrieve cfg_url in
  _get_keeper_config_generic fetcher cfg_url

let get_nursery_client keeper_id cli_cfg =
  let get_nc client =
    client # get_nursery_cfg () >>= fun ncfg ->
    Lwt.return ( Nursery.NC.make ncfg keeper_id Tcp_keepalive.default_tcp_keepalive)
  in
  with_master_remote_stream keeper_id cli_cfg get_nc


let __migrate_nursery_range config left sep right =
  Logger.debug_ "=== STARTING MIGRATE ===" >>= fun () ->
  get_keeper_config config >>= fun (keeper_id, cli_cfg) ->
  get_nursery_client keeper_id cli_cfg >>= fun nc ->
  Nursery.NC.migrate nc left sep right

let __init_nursery config cluster_id =
  Logger.info_ "=== STARTING INIT ===" >>= fun () ->
  get_keeper_config config >>= fun (keeper_id, cli_cfg) ->
  let set_routing client =
    Lwt.catch( fun () ->
        client # get_routing () >>= fun _cur ->
        failwith "Cannot initialize nursery. It's already initialized."
      ) ( function
        | Arakoon_exc.Exception( Arakoon_exc.E_NOT_FOUND, _ ) ->
          let r = Routing.build ( [], cluster_id ) in
          client # set_routing r
        | e -> Lwt.fail e
      )
  in
  with_master_remote_stream keeper_id cli_cfg set_routing


let __delete_from_nursery config cluster_id sep =
  Logger.info_ "=== STARTING DELETE ===" >>= fun () ->
  let m_sep =
    begin
      if sep = ""
      then None
      else Some sep
    end
  in
  get_keeper_config config >>= fun (keeper_id, cli_cfg) ->
  get_nursery_client keeper_id cli_cfg >>= fun nc ->
  Nursery.NC.delete nc cluster_id m_sep

let __main_run log_file f =
  Lwt_main.run(
    Lwt.catch
      ( fun () ->
         setup_logger log_file >>= fun () ->
         f () >>= fun () ->
         File_system.unlink log_file
      )
      ( fun e ->
         let msg = Printexc.to_string e in
         Logger.fatal_ msg >>= fun () ->
         Lwt.fail e)
  ) ; 0

let migrate_nursery_range config left sep right =
  __main_run "/tmp/nursery_migrate.log" ( fun() -> __migrate_nursery_range config left sep right )

let init_nursery config cluster_id =
  __main_run "/tmp/nursery_init.log" ( fun () -> __init_nursery config cluster_id )

let delete_nursery_cluster config cluster_id sep =
  __main_run "/tmp/nursery_delete.log" ( fun () -> __delete_from_nursery config cluster_id sep )
