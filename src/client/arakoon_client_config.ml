type cluster_id = string

type node_cfg = {
  ips : string list;
  port : int;
}

let show_node_cfg { ips; port } =
  Printf.sprintf "ips = %s, port = %i" (String.concat "|" ips) port

type ssl_cfg = {
  ca_cert : string;
  creds : (string * string) option;
  protocol : Ssl.protocol;
}

type t = {
  cluster_id : cluster_id;
  node_cfgs : (string * node_cfg) list;
  ssl_cfg : ssl_cfg option;
}
