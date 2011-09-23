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
open Ncfg
open Interval

let try_connect (ips, port) =
  Lwt.catch
    (fun () -> 
      let sa = Network.make_address ips port in
      Network.__open_connection sa >>= fun (ic,oc) ->
      let r = Some (ic,oc) in
      Lwt.return r
    )
    (fun exn -> Lwt.return None)



module NC = struct
  type connection = Lwt_io.input_channel * Lwt_io.output_channel 
  type lc = 
    | Address of (string * int)
    | Connection of connection

  type nn = string * string (* cluster_name,node_name *)

  type t = {
    rc : NCFG.t; 
    keeper_cn: string;
    connections : (nn ,lc) Hashtbl.t;
    masters: (string, string option) Hashtbl.t;
  }

  let make rc keeper_cn = 
    let masters = Hashtbl.create 5 in
    let () = NCFG.iter_cfgs rc (fun k _ -> Hashtbl.add masters k None) in
    let connections = Hashtbl.create 13 in
    let () = NCFG.iter_cfgs rc
      (fun cluster v -> 
	Hashtbl.iter (fun node (ip,port) ->
	  let nn = (cluster,node) in
	  let a = Address (ip,port) in
	  Hashtbl.add connections nn a) v)
    in
    {rc; connections;masters;keeper_cn}

  let _get_connection t nn = 
    let (cn,node) = nn in
    match Hashtbl.find t.connections nn with
      | Address (ip,port) -> 
	begin
	  try_connect (ip,port) >>= function
	    | Some conn -> 
	      Common.prologue cn conn >>= fun () ->
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
    Lwt_log.debug_f "_with_master_connection %s" cn >>= fun () ->
    _find_master t cn >>= fun master ->
    let nn = cn, master in
    _get_connection t nn >>= fun connection ->
    todo connection
    
  let set t key value = 
    let cn = NCFG.find_cluster t.rc key in
    Lwt_log.debug_f "set %S => %s" key cn >>= fun () ->
    let todo conn = Common.set conn key value in
    _with_master_connection t cn todo

  let get t key = 
    let cn = NCFG.find_cluster t.rc key in
    let todo conn = Common.get conn false key in
    _with_master_connection t cn todo

  let force_interval t cn i = 
    Lwt_log.debug_f "force_interval %s: %s" cn (Interval.to_string i) >>= fun () ->
    _with_master_connection t cn 
    (fun conn -> Common.set_interval conn i)


  let close t = Llio.lwt_failfmt "close not implemented"

  let migrate t start_key = (* direction is always 'up' *)
    Lwt_log.debug_f "migrate %s" start_key >>= fun () ->
    let from_cn = NCFG.find_cluster t.rc start_key in
    let to_cn = NCFG.next_cluster t.rc from_cn in
    Lwt_log.debug_f "from:%s to:%s" from_cn to_cn >>= fun () ->
    let pull () = 
      Lwt_log.debug "pull">>= fun () ->
      _with_master_connection t from_cn 
	(fun conn -> Common.get_tail conn start_key)
    in
    let push tail = 
      let seq = List.map (fun (k,v) -> Arakoon_client.Set(k,v)) tail in
      Lwt_log.debug "push" >>= fun () ->
      _with_master_connection t to_cn 
	(fun conn -> Common.sequence conn seq )
    in
    let delete tail = 
      let seq = List.map (fun (k,v) -> Arakoon_client.Delete k) tail in
      Lwt_log.debug "delete" >>= fun () ->
      _with_master_connection t from_cn 
	(fun conn -> Common.sequence conn seq)
    in
    let publish sep = 
      let route = NCFG.get_routing t.rc in      
      let new_route = Routing.change route from_cn sep to_cn in
      let () = NCFG.set_routing t.rc new_route in
      Lwt_log.debug_f "new route:%S" (Routing.to_s new_route) >>= fun () -> 
      _with_master_connection t t.keeper_cn
	(fun conn -> Common.set_routing_delta conn from_cn sep to_cn) 
      >>= fun () ->
      Lwt.return ()
    in
    let get_interval cn = _with_master_connection t cn Common.get_interval in
    let set_interval cn i = force_interval t cn i in
    let i2s i = Interval.to_string i in
    get_interval from_cn >>= fun from_i -> 
    get_interval to_cn >>= fun to_i ->
    let rec loop from_i to_i =
      pull () >>= fun tail ->
      match tail with
	| [] -> 
	  Lwt_log.debug "done" >>= fun () ->
	  Lwt.return ()
	| tail -> 
	  let size = List.length tail in
	  Lwt_log.debug_f "Length = %i" size >>= fun () ->
	  (* 
	     - change public interval on 'from'
	     - push tail & change private interval on 'to'
	     - delete tail & change private interval on 'from'
	     - change public interval 'to'
	     - publish new route.
	  *)
	  let (fpu_b,fpu_e),(fpr_b,fpr_e) = from_i in
	  let (tpu_b,tpu_e),(tpr_b,tpr_e) = to_i in
	  let b, _ = List.hd (List.rev tail) in
	  let e, _ = List.hd tail in
	  Lwt_log.debug_f "b:%S e:%S" b e >>= fun () ->
	  let from_i' = Interval.make fpu_b (Some b) fpr_b fpr_e in
	  set_interval from_cn from_i' >>= fun () ->
	  push tail >>= fun () ->
	  let to_i1 = Interval.make tpu_b tpu_e (Some b) tpr_e in
	  set_interval to_cn to_i1 >>= fun () ->
	  delete tail >>= fun () ->
	  let to_i2 = Interval.make (Some b) tpu_e (Some b) tpr_e in
	  set_interval to_cn to_i2 >>= fun () ->
	  let from_i2 = Interval.make fpu_b (Some b) fpr_b (Some b) in
	  set_interval from_cn from_i2 >>= fun () ->

	  Lwt_log.debug_f "from {%s:%s;%s:%s}" 
	    from_cn (i2s from_i) 
	    to_cn (i2s to_i) >>= fun () ->
	  Lwt_log.debug_f "to   {%s:%s;%s:%s}" 
	    from_cn (i2s from_i2) 
	    to_cn (i2s to_i2) >>= fun () ->

	  publish b >>= fun () ->
	  loop from_i2 to_i2
    in loop from_i to_i

end

let main () =
  All_test.configure_logging ();
  let repr = [("left", "ZZ")], "right" in (* all in left *)
  let routing = Routing.build repr in
  let left_cfg = ClientCfg.make () in
  let right_cfg = ClientCfg.make () in
  let () = ClientCfg.add left_cfg "left_0"   ("127.0.0.1", 4000) in
  let () = ClientCfg.add right_cfg "right_0" ("127.0.0.1", 5000) in
  let nursery_cfg = NCFG.make routing in
  let () = NCFG.add_cluster nursery_cfg "left" left_cfg in
  let () = NCFG.add_cluster nursery_cfg "right" right_cfg in
  let keeper = "left" in
  let nc = NC.make nursery_cfg keeper in
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
    Lwt_log.info "pre-fill" >>= fun () ->
    let rec fill i = 
      if i = 64 
      then Lwt.return () 
      else 
	let k = Printf.sprintf "%c" (Char.chr i) 
	and v = Printf.sprintf "%c_value" (Char.chr i) in
	NC.set nc k v >>= fun () -> 
	fill (i-1)
    in
    let left_i  = Interval.make None None None None    (* all *)
    and right_i = Interval.make None None None None in (* all *)
    NC.force_interval nc "left" left_i >>= fun () ->
    NC.force_interval nc "right" right_i >>= fun () ->
    fill 90 >>= fun () ->
    
    NC.migrate nc "T"
  in
  Lwt_main.run (t ())

let _ = main ();;
