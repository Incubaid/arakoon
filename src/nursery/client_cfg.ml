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

module ClientCfg = struct
  type sa = string * int
  type t = (string, sa) Hashtbl.t
  let cfg_to buf (t:t) = 
    let entry2 buf k (ip,p) = 
      Llio.string_to buf k;
      Llio.string_to buf ip;
      Llio.int_to buf p
    in
    Llio.hashtbl_to buf entry2 t

  let cfg_from buf pos = 
    let entry_from buf pos = 
      let k,p1 = Llio.string_from buf pos in
      let ip,p2 = Llio.string_from buf p1 in
      let p,p3 = Llio.int_from buf p2 in
      let (sa:sa) = ip,p in
      (k,sa),p3
    in
    Llio.hashtbl_from buf entry_from pos

  let node_names (t:t) = Hashtbl.fold (fun k v acc -> k::acc) t []
  let make () = Hashtbl.create 7
  let add (t:t) name sa = Hashtbl.add t name sa
  let get (t:t) name = Hashtbl.find t name


  let from_file fn =  (* This is the format as defined in the extension *)
    let inifile = new Inifiles.inifile fn in
    let cfg = make () in
    let _get s n p = Ini.get inifile s n p Ini.required in
    let cluster_id = _get "global" "cluster_id" Ini.p_string in
    let nodes      = _get "global" "cluster" Ini.p_string_list in
    let () = List.iter
      (fun n ->
	let ip = _get n "ip" Ini.p_string in
	let port = _get n "client_port" Ini.p_int in
	let sa = ip,port in
	add cfg n sa
      ) nodes
    in
    cfg

end

