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

  let input_cfg ic =
    let key_from ic =
      Llio.input_string ic 
    in
    let value_from ic =
      Llio.input_string ic >>= fun ip ->
      Llio.input_int ic >>= fun port ->
      Lwt.return (ip,port)
      
    in
    Llio.input_hashtbl key_from value_from ic 
    
  let output_cfg oc cfg =
    let helper oc key value =
      Llio.output_string oc key >>= fun () ->
      let (ip,port) = value in
      Llio.output_string oc ip >>= fun () ->
      Llio.output_int oc port 
    in
    Llio.output_hashtbl helper oc cfg
    
    
  let node_names (t:t) = Hashtbl.fold (fun k v acc -> k::acc) t []
  let make () = Hashtbl.create 7
  let add (t:t) name sa = Hashtbl.add t name sa
  let get (t:t) name = Hashtbl.find t name


  let from_file section fn =  (* This is the format as defined in the extension *)
    let inifile = new Inifiles.inifile fn in
    let cfg = make () in
    let _get s n p = Ini.get inifile s n p Ini.required in
    let nodes      = _get section "cluster" Ini.p_string_list in
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

