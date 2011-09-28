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

let with_remote_stream (cluster:string) cfg f =
  let host,port = cfg in
  let sa = Network.make_address host port in
  let do_it connection =
    Remote_nodestream.make_remote_nodestream cluster connection 
    >>= fun (client) ->
    f client
  in
    Lwt_io.with_connection sa do_it

let find_master cluster_id cli_cfg =
  let check_node node_name node_cfg acc = 
    begin
      Lwt_log.info_f "node=%s" node_name >>= fun () ->
      let (ip,port) = node_cfg in
      let sa = Network.make_address ip port in
      Lwt.catch
        (fun () ->
          Lwt_io.with_connection sa
            (fun connection ->
              Arakoon_remote_client.make_remote_client cluster_id  connection
            >>= fun client ->
            client # who_master ())
            >>= function
              | None -> acc
              | Some m -> Lwt_log.info_f "master=%s" m >>= fun () ->
                Lwt.return (Some m) )
        (function 
          | Unix.Unix_error(Unix.ECONNREFUSED,_,_ ) -> 
            Lwt_log.info_f "node %s is down, trying others" node_name >>= fun () ->
            acc
          | exn -> Lwt.fail exn
        )
    end
  in 
  Hashtbl.fold check_node cli_cfg (Lwt.return None) >>= function 
    | None -> failwith "No master found"
    | Some m -> Lwt.return m  

let with_master_remote_stream cluster_id cfg f =
  find_master cluster_id cfg >>= fun master_name ->
  let master_cfg = Hashtbl.find cfg master_name in
  with_remote_stream cluster_id master_cfg f


let __init_nursery config cluster_id = 
  let inifile = new Inifiles.inifile config in
  let m_cfg = Node_cfg.get_nursery_cfg inifile config in
  match m_cfg with
    | None -> failwith "No nursery keeper specified in config file"
    | Some (keeper_id, cli_cfg) -> 
      let set_routing client =
        Lwt.catch( fun () ->
          client # get_routing () >>= fun cur ->
          failwith "Cannot initialize nursery. It's already initialized."
        ) ( function
          | Arakoon_exc.Exception( Arakoon_exc.E_NOT_FOUND, _ ) ->
            print_string "Got routing\n"; 
            let r = Routing.build ( [], cluster_id ) in
            client # set_routing r 
          | e -> Lwt.fail e 
       ) 
      in
      with_master_remote_stream keeper_id cli_cfg set_routing
      
let init_nursery config cluster_id =
  Lwt_main.run( __init_nursery config cluster_id); 0
  
