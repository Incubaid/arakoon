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

open Lwt
open Node_cfg
open Nursery
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

let get_keeper_config config =
  let inifile = new Inifiles.inifile config in
  let m_cfg = Node_cfg.get_nursery_cfg inifile config in
  begin 
    match m_cfg with
      | None -> failwith "No nursery keeper specified in config file"
      | Some (keeper_id, cli_cfg) -> 
        keeper_id, cli_cfg
  end

let get_nursery_client keeper_id cli_cfg =
  let get_nc client =
    client # get_nursery_cfg () >>= fun ncfg ->
    Lwt.return ( NC.make ncfg keeper_id )
  in
  with_master_remote_stream keeper_id cli_cfg get_nc 


let __migrate_nursery_range config left sep right =
  Logger.debug_ "=== STARTING MIGRATE ===" >>= fun () ->
  let keeper_id, cli_cfg = get_keeper_config config in
  get_nursery_client keeper_id cli_cfg >>= fun nc ->
  NC.migrate nc left sep right 
    
let __init_nursery config cluster_id = 
  Logger.info_ "=== STARTING INIT ===" >>= fun () ->
  let (keeper_id, cli_cfg) = get_keeper_config config in
  let set_routing client =
    Lwt.catch( fun () ->
      client # get_routing () >>= fun cur ->
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
  let (keeper_id, cli_cfg) = get_keeper_config config in
  get_nursery_client keeper_id cli_cfg >>= fun nc ->
  NC.delete nc cluster_id m_sep 
  
let __main_run log_file f =
  Lwt_extra.run( 
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
    
