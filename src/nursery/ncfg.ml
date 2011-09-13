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

  let ncfg_from buf pos = 
    let r,p1 = Routing.routing_from buf pos in
    let ef (buf:string) pos = 
      let k,p2 = Llio.string_from buf pos in
      let v,p3 = ClientCfg.cfg_from buf p2 in
      (k,v),p3
    in
    let cfgs,p2 = Llio.hashtbl_from buf ef p1 in
    {r;cfgs},p2

  
  let make r = {r; cfgs = Hashtbl.create 17}
  let find_cluster t key = Routing.find t.r key
  let next_cluster t key = Routing.next t.r key
  let get_cluster  t name = Hashtbl.find t.cfgs name
  let add_cluster  t name cfg = Hashtbl.add t.cfgs name cfg
  let iter_cfgs t f = Hashtbl.iter f t.cfgs
  let get_routing t = t.r
  let set_routing t r = t.r <- r
end
