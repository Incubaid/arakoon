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

let with_client cfg (cluster:string) f =
  let host = cfg.ip
  and port = cfg.client_port in
  let sa = make_address host port in
  let do_it connection =
    Arakoon_remote_client.make_remote_client cluster connection 
    >>= fun (client: Arakoon_client.client) ->
    f client
  in
    Lwt_io.with_connection sa do_it

let find_master cluster_cfg =
  let cfgs = cluster_cfg.cfgs in
  let rec loop = function
    | [] -> Lwt.fail (Failure "too many nodes down")
    | cfg :: rest ->
      begin
	Lwt_log.info_f "cfg=%s" cfg.node_name >>= fun () ->
	let sa = make_address cfg.ip cfg.client_port in
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
	      | Some m -> Lwt_log.info_f "master=%s" m >>= fun () ->
		Lwt.return m)
	  (function 
	    | Unix.Unix_error(Unix.ECONNREFUSED,_,_ ) -> 
	      Lwt_log.info_f "node %s is down, trying others" cfg.node_name >>= fun () ->
	      loop rest
	    | exn -> Lwt.fail exn
	  )
      end
  in loop cfgs

let with_master_client cfg_name f =
  let ccfg = read_config cfg_name in
  let cfgs = ccfg.cfgs in
  let t () =
    find_master ccfg >>= fun master_name ->
    let master_cfg = List.hd (List.filter (fun cfg -> cfg.node_name = master_name) cfgs) in
    with_client master_cfg ccfg.cluster_id f
  in
  Lwt_main.run (t());0

let set cfg_name key value =
  with_master_client cfg_name (fun client -> client # set key value)

let get cfg_name key =
  let f (client:Arakoon_client.client) =
    client # get key >>= fun value ->
    Lwt_io.printlf "%S%!" value
  in
  with_master_client cfg_name f

let delete cfg_name key =
  with_master_client cfg_name (fun client -> client # delete key )

let benchmark cfg_name size tx_size max_n = 
  with_master_client cfg_name (Benchmark.benchmark ~size ~tx_size ~max_n)

let expect_progress_possible cfg_name =
  let f client = 
    client # expect_progress_possible () >>= fun b ->
    Lwt_io.printlf "%b" b
  in
  with_master_client cfg_name f


let statistics cfg_name =
  let f client =
    client # statistics () >>= fun statistics ->
    let rep = Statistics.string_of statistics in
    Lwt_io.printl rep
  in
  with_master_client cfg_name f

let who_master cfg_name () =
  let cluster_cfg = read_config cfg_name in
  let t () = 
    find_master cluster_cfg >>= fun master_name ->
    Lwt_io.printl master_name
  in
  Lwt_main.run (t());0
