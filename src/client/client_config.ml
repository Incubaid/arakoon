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

module ClusterId = struct
  type t = string
  let show t = Printf.sprintf "ClusterId %S" t
end

module NodeConfig = struct
  type t = { ips : string list;
             port : int;
             name : string;
           }

  let make ~ips ~port ~name = { ips; port; name; }

  let show t =
    let open To_string in
    record [ "name", string t.name;
             "ips", list string t.ips;
             "port", int t.port; ]
end

module ClusterClientConfig = struct
  type t = { id : ClusterId.t;
             nodes : NodeConfig.t list;
           }

  let show t =
    let open To_string in
    record [ "id", ClusterId.show t.id;
             "nodes", list NodeConfig.show t.nodes; ]
end
