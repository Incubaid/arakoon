(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
