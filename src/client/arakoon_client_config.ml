(*
Copyright 2016 iNuron NV

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

type cluster_id = string

type tcp_keepalive_cfg = Tcp_keepalive.t = {
      enable_tcp_keepalive : bool;
      tcp_keepalive_time : int;
      tcp_keepalive_intvl : int;
      tcp_keepalive_probes : int;
    }

let show_tcp_keepalive_cfg { enable_tcp_keepalive;
                             tcp_keepalive_time;
                             tcp_keepalive_intvl;
                             tcp_keepalive_probes; } =
  let open To_string in
  record
    [ "enable_tcp_keepalive", bool enable_tcp_keepalive;
      "tcp_keepalive_time", int tcp_keepalive_time;
      "tcp_keepalive_intvl", int tcp_keepalive_intvl;
      "tcp_keepalive_probes", int tcp_keepalive_probes; ]

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

let show_ssl_cfg { ca_cert; creds; protocol; } =
  Printf.sprintf "{ ca_cert=%s; creds=%s; protocol=%s; }"
                 ca_cert
                 (To_string.option
                    (To_string.pair Std.id Std.id)
                    creds)
                 (let open Ssl in
                  match protocol with
                  | SSLv23 -> "SSLv23"
                  | SSLv3 -> "SSLv3"
                  | TLSv1 -> "TLSv1"
                  | TLSv1_1 -> "TLSv1_1"
                  | TLSv1_2 -> "TLSv1_2"
                 )

type t = {
  cluster_id : cluster_id;
  node_cfgs : (string * node_cfg) list;
  ssl_cfg : ssl_cfg option;
  tcp_keepalive : tcp_keepalive_cfg;
}

let show { cluster_id; node_cfgs; ssl_cfg; _ } =
  Printf.sprintf
    "(cluster_id=%s, node_cfgs=%s, ssl_cfg=%s)"
    cluster_id
    (String.concat
       "; "
       (List.map
          (fun (node_name, node_cfg) ->
           Printf.sprintf "%s=%s" node_name (show_node_cfg node_cfg))
          node_cfgs))
    (To_string.option show_ssl_cfg ssl_cfg)

let get_node_cfg ({ node_cfgs; _ } as ccfg) node_name =
  try
    List.filter
      (fun (node_name', _cfg) -> node_name' = node_name)
      node_cfgs
    |> List.hd
    |> snd
  with _exn ->
    failwith (node_name ^ " is not known in config " ^ (show ccfg))

let tcp_keepalive_from_ini inifile =
  let enable_tcp_keepalive =
    Ini.get
      inifile "global" "enable_tcp_keepalive"
      Ini.p_bool (Ini.default true)
  in
  let tcp_keepalive_time =
    Ini.get
      inifile "global" "tcp_keepalive_time"
      Ini.p_int (Ini.default 20)
  in
  let tcp_keepalive_intvl =
    Ini.get
      inifile "global" "tcp_keepalive_intvl"
      Ini.p_int (Ini.default 20)
  in
  let tcp_keepalive_probes =
    Ini.get
      inifile "global" "tcp_keepalive_probes"
      Ini.p_int (Ini.default 3)
  in
  { enable_tcp_keepalive;
    tcp_keepalive_time;
    tcp_keepalive_intvl;
    tcp_keepalive_probes;
  }

let from_ini txt =
  let inifile = new Arakoon_inifiles.inifile txt in
  let cluster_id = Ini.get inifile "global" "cluster_id" Ini.p_string Ini.required in
  let nodes = Ini.get inifile "global" "cluster" Ini.p_string_list Ini.required in
  let node_cfgs =
    List.map
      (fun node_name ->
       let ips = Ini.get inifile node_name "ip" Ini.p_string_list Ini.required in
       let port = Ini.get inifile node_name "client_port" Ini.p_int Ini.required in
       (node_name, { ips; port; }))
      nodes
  in
  let tls_ca_cert = Ini.get inifile
                            "global" "tls_ca_cert"
                            (Ini.p_option Ini.p_string)
                            (Ini.default None)
  in
  let tls_cert = Ini.get inifile
                         "global" "tls_cert"
                         (Ini.p_option Ini.p_string)
                         (Ini.default None)
  in
  let tls_version = Ini.get inifile
                            "global" "tls_version"
                            (Ini.p_option Ini.p_string)
                            (Ini.default None)
  in
  let tls_key = Ini.get inifile
                        "global" "tls_key"
                        (Ini.p_option Ini.p_string)
                        (Ini.default None)
  in
  let ssl_cfg =
    match tls_ca_cert, tls_version with
    | None, _
    | _, None -> None
    | Some ca_cert, Some tls_version ->
       let protocol = match tls_version with
         | "1.0" -> Ssl.TLSv1
         | "1.1" -> Ssl.TLSv1_1
         | "1.2" -> Ssl.TLSv1_2
         | _ -> failwith "unsuppored tls protocol version"
       in
       let creds = match tls_cert, tls_key with
         | None, _
         | _, None -> None
         | Some s, Some k -> Some (s, k)
       in
       Some { ca_cert;
              creds;
              protocol; }
  in
  { cluster_id;
    node_cfgs;
    ssl_cfg;
    tcp_keepalive = tcp_keepalive_from_ini inifile;
  }


let to_ini { cluster_id;
             node_cfgs;
             ssl_cfg;
             tcp_keepalive; } =
  let inifile = new Arakoon_inifiles.inifile "[global]\ncluster_id = x" in

  inifile # delval "global" "cluster_id";

  let node_names =
    List.fold_left
      (fun acc (node_name, { ips; port; }) ->
       inifile # setval node_name "ip" (String.concat ", " ips);
       inifile # setval node_name "client_port" (string_of_int port);
       node_name :: acc)
      []
      node_cfgs
  in

  inifile # setval "global" "cluster_id" cluster_id;
  inifile # setval "global" "cluster" (String.concat ", " node_names);

  let { enable_tcp_keepalive;
        tcp_keepalive_time;
        tcp_keepalive_intvl;
        tcp_keepalive_probes; } = tcp_keepalive in
  inifile # setval "global" "enable_tcp_keepalive" (string_of_bool enable_tcp_keepalive);
  inifile # setval "global" "tcp_keepalive_time"   (string_of_int  tcp_keepalive_time);
  inifile # setval "global" "tcp_keepalive_intvl"  (string_of_int  tcp_keepalive_intvl);
  inifile # setval "global" "tcp_keepalive_probes" (string_of_int  tcp_keepalive_probes);

  let () = match ssl_cfg with
    | None -> ()
    | Some { ca_cert;
             creds;
             protocol; } ->
       inifile # setval "global" "tls_ca_cert" ca_cert;
       let () = match creds with
         | None -> ()
         | Some (tls_cert, tls_key) ->
            inifile # setval "global" "tls_cert" tls_cert;
            inifile # setval "global" "tls_key"  tls_key
       in
       inifile # setval
               "global" "tls_version"
               (match protocol with
                | Ssl.TLSv1   -> "1.0"
                | Ssl.TLSv1_1 -> "1.1"
                | Ssl.TLSv1_2 -> "1.2"
                | Ssl.SSLv23
                | Ssl.SSLv3 -> failwith "unsupported ssl version")
  in

  inifile # to_string
