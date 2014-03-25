(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

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

open Routing
open Client_cfg

module NCFG = struct
  type t = {mutable r: Routing.t;
            cfgs : (string, ClientCfg.t) Hashtbl.t}

  let ncfg_to buf (r,cs) =
    Routing.routing_to buf r;
    let e2 buf k v =
      Llio.string_to buf k;
      ClientCfg.cfg_to buf v
    in
    Llio.hashtbl_to buf e2 cs

  let ncfg_from buf =
    let r = Routing.routing_from buf in
    let ef buf =
      let k = Llio.string_from buf in
      let v = ClientCfg.cfg_from buf in
      (k,v)
    in
    let cfgs = Llio.hashtbl_from buf ef in
    {r;cfgs}


  let make r = {r; cfgs = Hashtbl.create 17}
  let find_cluster t key = Routing.find t.r key
  let next_cluster t key = Routing.next t.r key
  let get_cluster  t name = Hashtbl.find t.cfgs name
  let add_cluster  t name cfg = Hashtbl.add t.cfgs name cfg
  let iter_cfgs t f = Hashtbl.iter f t.cfgs
  let get_routing t = t.r
  let set_routing t r = t.r <- r

end
