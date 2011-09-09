open Routing
open Client_cfg

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
