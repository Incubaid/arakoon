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

let show { cluster_id; node_cfgs; ssl_cfg; } =
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
  with exn ->
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
  let inifile = new Inifiles.inifile txt in
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
