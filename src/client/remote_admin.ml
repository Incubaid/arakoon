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

open Interval
open Routing
open Common
open Lwt
open Client_cfg
open Ncfg

class type admin = object
      
  method compact: int -> unit Lwt.t 

  method set_routing: Routing.t -> unit Lwt.t
  method set_routing_delta: string -> string -> string -> unit Lwt.t
  method get_routing: unit -> Routing.t Lwt.t
  
  method get_db: string -> unit Lwt.t

  method get_fringe: string option -> Routing.range_direction -> ((string * string) list) Lwt.t
  method set_interval : Interval.t -> unit Lwt.t
  method get_interval : unit -> Interval.t Lwt.t
  
  method store_cluster_cfg : string -> ClientCfg.t -> unit Lwt.t 
  
  method get_nursery_cfg: unit -> NCFG.t Lwt.t
end

open Baardskeerder
class remote_admin ((ic,oc) as conn) = object(self :# admin)
  method compact n = 
    Common.request oc (fun buf -> 
      Common.command_to buf COMPACT;
      Pack.vint_to buf n) 
    >>= fun () ->
    Common.response_limited ic (fun _ -> ())

  method set_interval iv = Common.set_interval conn iv
  method get_interval () = Common.get_interval conn 

  method get_routing () = Common.get_routing conn

  method set_routing r = Common.set_routing conn r
   
  method set_routing_delta left sep right = Common.set_routing_delta conn left sep right
    
  method get_db db_location =
    
    let outgoing buf =
      command_to buf GET_DB;
    in
    let incoming ic =
      Llio.input_int64 ic >>= fun length -> 
      Lwt_io.with_file ~mode:Lwt_io.output db_location (fun oc -> Llio.copy_stream ~length ~ic ~oc)
    in
    request  oc outgoing >>= fun () ->
    response_old ic incoming

  method get_fringe (boundary:string option) direction= 
    Common.get_fringe conn boundary direction
  
  method store_cluster_cfg cluster_id cfg =
    Common.set_nursery_cfg (ic,oc) cluster_id cfg
  
  method get_nursery_cfg () = 
    Common.get_nursery_cfg (ic,oc)
end

let make cluster connection = 
  prologue cluster connection >>= fun () ->
  let rns = new remote_admin connection in
  let a = (rns :> admin) in
  Lwt.return a
  
 
