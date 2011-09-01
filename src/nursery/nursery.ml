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
open Routing
open Client_cfg

let try_connect (ips, port) =
  Lwt.catch
    (fun () -> 
      let sa = Network.make_address ips port in
      Network.__open_connection sa >>= fun (ic,oc) ->
      let r = Some (ic,oc) in
      Lwt.return r
    )
    (fun exn -> Lwt.return None)

module NCFG = struct
  type t = Routing.t * (string, ClientCfg.t) Hashtbl.t

  let ncfg_to buf (r,cs) = 
    Routing.routing_to buf r;
    let e2 buf k v = 
      Llio.string_to buf k;
      ClientCfg.cfg_to buf v
    in
    Llio.hashtbl_to buf e2 cs

  let ncfg_from buf pos = 
    let r,p1 = Routing.routing_from buf pos in
    let ef (buf:string) pos = 
      let k,p2 = Llio.string_from buf pos in
      let v,p3 = ClientCfg.cfg_from buf p2 in
      (k,v),p3
    in
    let c,p2 = Llio.hashtbl_from buf ef p1 in
    (r,c),p2

  
  let make r = (r, Hashtbl.create 17)
  let find_cluster (r,_) key = Routing.find r key
  let next_cluster (r,_) key = Routing.next r key
  let get_cluster (_,c) name = Hashtbl.find c name
  let add_cluster (_,c) name  cfg = Hashtbl.add c name cfg
end

module NC = struct
  type connection = Lwt_io.input_channel * Lwt_io.output_channel 
  type lc = 
    | Address of (string * int)
    | Connection of connection

  type nn = string * string (* cluster_name,node_name *)

  type t = {
    rc : NCFG.t; 
    connections : (nn ,lc) Hashtbl.t;
    masters: (string, string option) Hashtbl.t;
  }

  let make rc = 
    let (_,c) = rc in
    let masters = Hashtbl.create 5 in
    let () = Hashtbl.iter (fun k _ -> Hashtbl.add masters k None) c in
    let connections = Hashtbl.create 13 in
    let () = Hashtbl.iter 
      (fun cluster v -> 
	Hashtbl.iter (fun node (ip,port) ->
	  let nn = (cluster,node) in
	  let a = Address (ip,port) in
	  Hashtbl.add connections nn a) v)
      c
    in
    {rc; connections;masters;}

  let _get_connection t nn = 
    let (cn,node) = nn in
    Lwt_log.debug_f "_get_connection(%s,%s)" cn node >>= fun () ->
    match Hashtbl.find t.connections nn with
      | Address (ip,port) -> 
	begin
	  try_connect (ip,port) >>= function
	    | Some conn -> 
	      let () = Hashtbl.add t.connections nn (Connection conn) in
	      Lwt.return conn
	    | None -> Llio.lwt_failfmt "Connection to (%s,%i) failed" ip port
	end
      | Connection conn -> Lwt.return conn
  
  let _find_master_remote t cn = 
    let ccfg = NCFG.get_cluster t.rc cn in
    let node_names = ClientCfg.node_names ccfg in
    Lwt_log.debug_f "names:%s" (Log_extra.string_of_list (fun s->s) node_names) 
    >>= fun () ->
    Lwt_list.map_p 
      (fun n -> 
	let nn = (cn,n) in 
	_get_connection t nn
      ) 
      node_names 
    >>= fun connections ->
    Lwt.choose (List.map (fun conn -> Common.who_master conn) connections) 
    >>= function
      | None -> Llio.lwt_failfmt "no master for %s" cn
      | Some master -> Lwt.return master
    
	
  let _find_master t cn = 
    let m = Hashtbl.find t.masters cn in
    match m with
      | None -> _find_master_remote t cn 
      | Some master -> Lwt.return master 	  

  let _with_master_connection t cn (todo: connection -> 'c Lwt.t) =
    _find_master t cn >>= fun master ->
    let nn = cn, master in
    _get_connection t nn >>= fun connection ->
    todo connection
    
  let set t key value = 
    let cn = NCFG.find_cluster t.rc key in
    let todo conn = Common.set conn key value in
    _with_master_connection t cn todo

  let get t key = 
    let cn = NCFG.find_cluster t.rc key in
    let todo conn = Common.get conn false key in
    _with_master_connection t cn todo

  let close t = Llio.lwt_failfmt "close not implemented"


  let migrate t start_key = (* direction is always 'up' *)
    let from_cn = NCFG.find_cluster t.rc start_key in
    let to_cn = NCFG.next_cluster t.rc from_cn in
    let todo conn = Common.get_tail conn start_key in
    let rec loop () =
      _with_master_connection t from_cn todo >>= function
	| [] -> 
	  Lwt_io.printlf "done" >>= fun () ->
	  Lwt.return ()
	| tail -> 
	  let size = List.length tail in
	  Lwt_io.printlf "Length = %i" size >>= fun () ->
	  loop ()
    in loop ()

end

let main () =
  All_test.configure_logging ();
  let repr = [("left", "")], "right" in (* all in left *)
  let routing = Routing.build repr in
  let left_cfg = ClientCfg.make () in
  let right_cfg = ClientCfg.make () in
  let () = ClientCfg.add left_cfg "1" ("127.0.0.1", 4000) in
  let () = ClientCfg.add right_cfg "one" ("127.0.0.1", 5000) in
  let nursery_cfg = NCFG.make routing in
  let () = NCFG.add_cluster nursery_cfg "left" left_cfg in
  let () = NCFG.add_cluster nursery_cfg "right" right_cfg in
  let nc = NC.make nursery_cfg in
  (*
  let test k v = 
    NC.set client k v >>= fun () ->
    NC.get client k >>= fun v' ->
    Lwt_log.debug_f "get '%s' yields %s" k v' >>= fun () ->
    Lwt_log.debug "done"
  in
  let t () = 
    test "a" "A" >>= fun () ->
    test "z" "Z" 
  *)
  let t () = 
    let rec fill i = 
      if i = 64 
      then Lwt.return () 
      else 
	let k = Printf.sprintf "%c" (Char.chr i) 
	and v = Printf.sprintf "%c_value" (Char.chr i) in
	NC.set nc k v >>= fun () ->
	fill (i-1)
    in
    fill 90 >>= fun () ->
    NC.migrate nc "t"
  in
  Lwt_main.run (t ())

let _ = main ();;
