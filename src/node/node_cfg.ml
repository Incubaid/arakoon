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



let section = Logger.Section.main

let config_file = ref "cfg/arakoon.ini"

let default_lease_period = 10
let default_max_value_size = 8 * 1024 * 1024
let default_max_buffer_size = 32 * 1024 * 1024
let default_client_buffer_capacity = 32
let default_lcnum = 16384
let default_ncnum = 8192
let default_head_copy_throttling = 0.0
open Master_type
open Client_cfg

exception InvalidHomeDir of string
exception InvalidTlogDir of string
exception InvalidTlxDir of string
exception InvalidHeadDir of string

module TLSConfig = struct
    type path = string
    type cipher_list = string

    module Cluster : sig
        type t

        val make : ca_cert:path
                 -> service:bool
                 -> service_validate_peer:bool
                 -> protocol:Ssl.protocol
                 -> cipher_list:cipher_list option
                 -> t
        val ca_cert : t -> path
        val service : t -> bool
        val service_validate_peer : t -> bool
        val protocol : t -> Ssl.protocol
        val cipher_list : t -> cipher_list option

        val to_string : t -> string
    end = struct
        type t = { ca_cert : path
                 ; service : bool
                 ; service_validate_peer : bool
                 ; protocol : Ssl.protocol
                 ; cipher_list : cipher_list option
                 }

        let make ~ca_cert ~service ~service_validate_peer ~protocol ~cipher_list =
            { ca_cert; service; service_validate_peer; protocol; cipher_list }

        let ca_cert t = t.ca_cert
        let service t = t.service
        let service_validate_peer t = t.service_validate_peer
        let protocol t = t.protocol
        let cipher_list t = t.cipher_list

        let to_string t =
            let open To_string in
            record [ "ca_cert", string t.ca_cert
                   ; "service", bool t.service
                   ; "service_validate_peer", bool t.service_validate_peer
                   ; "protocol", begin match t.protocol with
                     | Ssl.SSLv23 -> "SSLv23"
                     | Ssl.SSLv3 -> "SSLv3"
                     | Ssl.TLSv1 -> "TLSv1"
                     | Ssl.TLSv1_1 -> "TLSv1_1"
                     | Ssl.TLSv1_2 -> "TLSv1_2"
                   end
                   ; "cipher_list", option string t.cipher_list
                   ]
    end

    module Node : sig
        type t

        val make : cert:path -> key:path -> t

        val cert : t -> path
        val key : t -> path

        val to_string : t -> string
    end = struct
        type t = { cert : path
                 ; key : path
                 }

        let make ~cert ~key = { cert; key }
        let cert t = t.cert
        let key t = t.key

        let to_string t =
            let open To_string in
            record [ "cert", string t.cert
                   ; "key", string t.key
                   ]
    end
end

module Node_cfg = struct

  type t = {node_name:string;
            ips: string list;
            client_port:int;
            messaging_port:int;
            home:string;
            tlog_dir:string;
            tlx_dir:string;
            head_dir:string;
            log_dir:string;
            log_level:string;
            log_config:string option;
            batched_transaction_config:string option;
            lease_period:int;
            master: master;
            is_laggy : bool;
            is_learner : bool;
            is_witness : bool;
            targets : string list;
            compressor : Compression.compressor;
            fsync : bool;
            _fsync_tlog_dir : bool;
            is_test : bool;
            reporting: int;
            node_tls : TLSConfig.Node.t option;
            collapse_slowdown : float option;
            head_copy_throttling: float;
           }

  let string_of t =
    let open To_string in
    record [ "node_name", string t.node_name
           ; "ips", list string t.ips
           ; "client_port", int t.client_port
           ; "messaging_port", int t.messaging_port
           ; "home", string t.home
           ; "tlog_dir", string t.tlog_dir
           ; "log_dir", string t.log_dir
           ; "tlx_dir", string t.tlx_dir
           ; "head_dir", string t.head_dir
           ; "log_level", string t.log_level
           ; "log_config", option string t.log_config
           ; "batched_transaction_config", option string t.batched_transaction_config
           ; "lease_period", int t.lease_period
           ; "master", string (master2s t.master)
           ; "is_laggy", bool t.is_laggy
           ; "is_learner", bool t.is_learner
           ; "is_witness", bool t.is_witness
           ; "targets", list id t.targets
           ; "compressor", Compression.compressor2s t.compressor
           ; "fsync", bool t.fsync
           ; "is_test", bool t.is_test
           ; "reporting", int t.reporting
           ; "_fsync_tlog_dir", bool t._fsync_tlog_dir
           ; "node_tls", option TLSConfig.Node.to_string t.node_tls
           ; "collapse_slowdown", option To_string.float t.collapse_slowdown
           ; "head_copy_throttling", To_string.float t.head_copy_throttling
           ]

  type log_cfg =
    {
      client_protocol: string option;
      paxos: string option;
      tcp_messaging: string option;
    }

  let string_of_log_cfg lcfg =
    let open To_string in
    record [ "client_protocol", option string lcfg.client_protocol
           ; "paxos", option string lcfg.paxos
           ; "tcp_messaging", option string lcfg.tcp_messaging
           ]

  let get_default_log_config () =
    {
      client_protocol = None;
      paxos = None;
      tcp_messaging = None;
    }

  type batched_transaction_cfg =
    {
      max_entries : int option;
      max_size : int option;
    }

  let string_of_btc (btc:batched_transaction_cfg) =
    let open To_string in
    record [ "max_entries", option int btc.max_entries
           ; "max_size", option int btc.max_size
           ]

  let get_default_batched_transaction_config () =
    {
      max_entries = None;
      max_size = None;
    }

  type cluster_cfg =
    {
      cfgs: t list;
      log_cfgs: (string * log_cfg) list;
      batched_transaction_cfgs : (string * batched_transaction_cfg) list;
      _master: master;
      quorum_function: int -> int;
      _lease_period: int;
      cluster_id : string;
      plugins: string list;
      nursery_cfg : (string*ClientCfg.t) option;
      overwrite_tlog_entries: int option;
      max_value_size: int;
      max_buffer_size: int;
      client_buffer_capacity: int;
      lcnum : int; (* tokyo cabinet: leaf nodes in cache *)
      ncnum : int; (* tokyo cabinet: internal nodes in cache *)
      tls : TLSConfig.Cluster.t option;
}

  let string_of_cluster_cfg c =
    let open To_string in
    record [ "cfgs", list string_of c.cfgs
           ; "log_cfgs", list (pair string string_of_log_cfg) c.log_cfgs
           ; "batched_transaction_cfgs", list (pair string string_of_btc) c.batched_transaction_cfgs
           ; "_master", master2s c._master
           ; "_lease_period", int c._lease_period
           ; "cluster_id", string c.cluster_id
           ; "plugins", list string c.plugins
           ; "nursery_cfg", option (pair string ClientCfg.to_string) c.nursery_cfg
           ; "overwrite_tlog_entries", option int c.overwrite_tlog_entries
           ; "max_value_size", int c.max_value_size
           ; "max_buffer_size", int c.max_buffer_size
           ; "client_buffer_capacity", int c.client_buffer_capacity
           ; "lcnum", int c.lcnum
           ; "ncnum", int c.ncnum
           ; "tls", option TLSConfig.Cluster.to_string c.tls
           ]

  let make_test_config
        ?(base=4000) ?(cluster_id="ricky") ?(node_name = Printf.sprintf "t_arakoon_%i")
        n_nodes master lease_period =
    let make_one n =
      let ns = (string_of_int n) in
      let home = ":MEM#t_arakoon_" ^ ns in
      {
        node_name = node_name n;
        ips = ["127.0.0.1"];
        client_port = (base + n);
        messaging_port = (base + 10 + n);
        home = home;
        tlog_dir = home;
        tlx_dir = home;
        head_dir = home;
        log_dir = ":None";
        log_level = "DEBUG";
        log_config = Some "default_log_config";
        batched_transaction_config = Some "default_batched_transaction_config";
        lease_period = lease_period;
        master = master;
        is_laggy = false;
        is_learner = false;
        is_witness = false;
        targets = [];
        compressor = Compression.Snappy;
        fsync = false;
        _fsync_tlog_dir = false;
        is_test = true;
        reporting = 300;
        node_tls = None;
        collapse_slowdown = None;
        head_copy_throttling = default_head_copy_throttling;
      }
    in
    let rec loop acc = function
      | 0 -> acc
      | n -> let o = make_one (n-1) in
        loop (o::acc) (n-1)
    in
    let log_cfgs = [("default_log_config", get_default_log_config ())] in
    let batched_transaction_cfgs = [("default_batched_transaction_config", get_default_batched_transaction_config ())] in
    let cfgs = loop [] n_nodes in
    let quorum_function = Quorum.quorum_function in
    let overwrite_tlog_entries = None in
    let cluster_cfg = {
      cfgs;
      log_cfgs;
      batched_transaction_cfgs;
      nursery_cfg = None;
      _master = master;
      quorum_function;
      _lease_period = default_lease_period;
      cluster_id;
      plugins = [];
      overwrite_tlog_entries;
      max_value_size = default_max_value_size;
      max_buffer_size = default_max_buffer_size;
      client_buffer_capacity = default_client_buffer_capacity;
      lcnum = default_lcnum;
      ncnum = default_ncnum;
      tls = None;
    }
    in
    cluster_cfg




  let _node_names inifile =
    Ini.get inifile "global" "cluster" Ini.p_string_list Ini.required

  let _ips inifile node_name =
    Ini.get inifile node_name "ip" Ini.p_string_list Ini.required

  let _tlog_entries_overwrite inifile =
    Ini.get inifile "global" "__tainted_tlog_entries_per_file"
      (Ini.p_option Ini.p_int )
      (Ini.default None)

  let _max_value_size inifile =
    Ini.get inifile "global" "__tainted_max_value_size"
      Ini.p_int (Ini.default (8 * 1024 * 1024))

  let _max_buffer_size inifile =
    Ini.get inifile "global" "__tainted_max_buffer_size"
      Ini.p_int (Ini.default default_max_buffer_size)

  let _plugins inifile =
    Ini.get inifile "global" "plugins" Ini.p_string_list (Ini.default [])

  let _get_lease_period inifile =
    Ini.get inifile "global" "lease_period"
      Ini.p_int (Ini.default default_lease_period)

  let _get_bool inifile node_section x =
    Ini.get inifile node_section x Ini.p_bool (Ini.default false)

  let _client_buffer_capacity inifile =
    Ini.get inifile "global" "client_buffer_capacity"
      Ini.p_int (Ini.default default_client_buffer_capacity)

  let _lcnum inifile =
    Ini.get inifile "global" "lcnum" Ini.p_int
      (Ini.default default_lcnum)

  let _ncnum inifile =
    Ini.get inifile "global" "ncnum" Ini.p_int
      (Ini.default default_ncnum)

  let _startup_mode inifile =
    let master =
      try
        let m_s = (inifile # getval "global" "master") in
        if Ini.get inifile "global" "preferred_masters" (fun _ -> true) (fun _ _ -> false)
        then
          failwith ("'master' and 'preferred_masters' are incompatible")
        else
          let m = Scanf.sscanf m_s "%s" (fun s -> s) in
          let nodes = _node_names inifile in
          if not (List.mem m nodes)
          then
            failwith (Printf.sprintf "'%s' needs to have a config section [%s]" m m)
          else
          if _get_bool inifile "global" "preferred_master"
          then (Preferred [m])
          else (Forced m)
      with (Inifiles.Invalid_element _) ->
        let pms = Ini.get inifile "global" "preferred_masters" Ini.p_string_list (fun _ _ -> []) in
        if pms <> []
        then
          Preferred pms
        else
          let read_only = _get_bool inifile "global" "readonly" in
          if read_only
          then ReadOnly
          else Elected
    in
    master

  let get_nursery_cfg inifile filename =
    try
      begin
        let n_cluster_id = Ini.get inifile "nursery" "cluster_id" Ini.p_string Ini.required in
        let cfg =  ClientCfg.from_file "nursery" filename in
        Some (n_cluster_id, cfg)
      end
    with _ -> None

  let _get_cluster_id inifile =
    try
      let cids = inifile # getval "global" "cluster_id" in
      Scanf.sscanf cids "%s" (fun s -> s)
    with (Inifiles.Invalid_element _ ) -> failwith "config has no cluster_id"

  let _get_quorum_function inifile =
    let nodes = _node_names inifile in
    let n_nodes = List.length nodes in
    try
      let qs = (inifile # getval "global" "quorum") in
      let qi = Scanf.sscanf qs "%i" (fun i -> i) in
      if 1 <= qi && qi <= n_nodes
      then fun _n -> qi
      else
        let msg = Printf.sprintf "fixed quorum should be 1 <= %i <= %i"
                    qi n_nodes in
        failwith msg
    with (Inifiles.Invalid_element _) -> Quorum.quorum_function

  let _log_config inifile log_name =
    let get_log_level x =
      let v = Ini.get inifile log_name x (Ini.p_option Ini.p_string) (Ini.default None) in
      match v with
        | None -> None
        | Some v -> Some (String.lowercase v) in
    let client_protocol = get_log_level "client_protocol" in
    let paxos = get_log_level "paxos" in
    let tcp_messaging = get_log_level "tcp_messaging" in
    {
      client_protocol;
      paxos;
      tcp_messaging;
    }

  let _batched_transaction_config inifile section_name =
    let get_option_int x = Ini.get inifile section_name x (Ini.p_option Ini.p_int) (Ini.default None) in
    let max_entries = get_option_int "max_entries" in
    let max_size = get_option_int "max_size" in
    {
      max_entries;
      max_size;
    }

  let _tls_ca_cert inifile =
    Ini.get inifile "global" "tls_ca_cert" (Ini.p_option Ini.p_string) (Ini.default None)

  let _tls_service inifile =
    Ini.get inifile "global" "tls_service"
      Ini.p_bool (Ini.default false)

  let _tls_service_validate_peer inifile =
    Ini.get inifile "global" "tls_service_validate_peer"
      Ini.p_bool (Ini.default false)

  let _tls_version inifile =
    let s = Ini.get inifile "global" "tls_version" Ini.p_string (Ini.default "1.0") in
    match s with
      | "1.0" -> Ssl.TLSv1
      | "1.1" -> Ssl.TLSv1_1
      | "1.2" -> Ssl.TLSv1_2
      | _ -> failwith "Invalid \"tls_version\" setting"

  let _tls_cipher_list inifile =
    Ini.get inifile "global" "tls_cipher_list"
      (Ini.p_option Ini.p_string) (Ini.default None)

  let _node_config inifile node_name master =
    let get_string x = Ini.get inifile node_name x Ini.p_string Ini.required in
    let get_bool x = _get_bool inifile node_name x in
    let get_int x = Ini.get inifile node_name x Ini.p_int Ini.required in
    let ips = _ips inifile node_name in
    let client_port = get_int "client_port" in
    let messaging_port = get_int "messaging_port" in
    let home = get_string "home" in
    let tlog_dir =
      try get_string "tlog_dir"
      with _ -> home
    in
    let tlx_dir =
      let rec _find = function
        | [] -> tlog_dir
        | x :: xs -> try get_string x
                     with _ -> _find xs
      in
      _find ["tlx_dir";"tlf_dir"]
    in

    let head_dir =
      try get_string "head_dir"
      with _ -> tlx_dir
    in
    let log_level = String.lowercase (get_string "log_level")  in
    let log_config = Ini.get inifile node_name "log_config" (Ini.p_option Ini.p_string) (Ini.default None) in
    let batched_transaction_config = Ini.get inifile node_name "batched_transaction_config" (Ini.p_option Ini.p_string) (Ini.default None) in
    let is_laggy = get_bool "laggy" in
    let is_learner = get_bool "learner" in
    let is_witness = get_bool "witness" in
    let compressor =
      if get_bool "disable_tlog_compression"
      then Compression.No
      else
        begin
          let s = Ini.get inifile node_name "tlog_compression" Ini.p_string
                          (Ini.default "bz2")
          in
          match String.lowercase s with
          | "snappy"  -> Compression.Snappy
          | "bz2"     -> Compression.Bz2
          | "none"    -> Compression.No
          | _         -> failwith (Printf.sprintf "no compressor named %s" s)
        end
    in
    let fsync = Ini.get inifile node_name "fsync" Ini.p_bool (Ini.default true) in
    let _fsync_tlog_dir = Ini.get
                            inifile
                            node_name
                            "__tainted_fsync_tlog_dir"
                            Ini.p_bool
                            (Ini.default true) in
    let targets =
      if is_learner
      then Ini.get inifile node_name "targets" Ini.p_string_list Ini.required
      else []
    in
    let lease_period = _get_lease_period inifile in
    let log_dir =
      try get_string "log_dir"
      with _ -> home
    in
    let reporting = Ini.get inifile node_name "reporting" Ini.p_int (Ini.default 300) in
    let get_string_option x = Ini.get inifile node_name x (Ini.p_option Ini.p_string) (Ini.default None) in
    let tls_cert = get_string_option "tls_cert"
    and tls_key = get_string_option "tls_key" in
    let node_tls =
      let msg = Printf.sprintf "%s: both tls_cert and tls_key should be provided" node_name in
      match tls_cert with
        | None -> begin match tls_key with
            | None -> None
            | Some _ -> failwith msg
        end
        | Some cert -> begin match tls_key with
            | None -> failwith msg
            | Some key -> Some (TLSConfig.Node.make ~cert ~key)
        end
    in
    let collapse_slowdown = Ini.get inifile node_name "collapse_slowdown" (Ini.p_option Ini.p_float) (Ini.default None) in
    let head_copy_throttling =
      Ini.get inifile node_name "head_copy_throttling"
              Ini.p_float (Ini.default default_head_copy_throttling)
    in
    {node_name;
     ips;
     client_port;
     messaging_port;
     home;
     tlog_dir;
     tlx_dir;
     head_dir;
     log_dir;
     log_level;
     log_config;
     batched_transaction_config;
     lease_period;
     master;
     is_laggy;
     is_learner;
     is_witness;
     targets;
     compressor;
     fsync;
     _fsync_tlog_dir;
     is_test = false;
     reporting;
     node_tls;
     collapse_slowdown;
     head_copy_throttling;
    }


  let read_config config_file =
    let inifile = new Inifiles.inifile config_file in
    let fm = _startup_mode inifile in
    let nodes = _node_names inifile in
    let plugin_names = _plugins inifile in
    let cfgs, remaining = List.fold_left
                            (fun (a,remaining) section ->
                               if List.mem section nodes || _get_bool inifile section "learner"
                               then
                                 let cfg = _node_config inifile section fm in
                                 let new_remaining = List.filter (fun x -> x <> section) remaining in
                                 (cfg::a, new_remaining)
                               else (a,remaining))
                            ([],nodes) (inifile # sects) in
    let log_cfg_names = List.map (fun cfg -> cfg.log_config) cfgs in
    let log_cfgs = List.fold_left
                     (fun a section ->
                        if List.mem (Some section) log_cfg_names
                        then
                          let log_cfg = _log_config inifile section in
                          (section, log_cfg)::a
                        else
                          a)
                     [] (inifile # sects) in
    let batched_transaction_cfg_names = List.map (fun cfg -> cfg.batched_transaction_config) cfgs in
    let batched_transaction_cfgs = List.fold_left
                                     (fun a section ->
                                        if List.mem (Some section) batched_transaction_cfg_names
                                        then
                                          let batched_transaction_cfg = _batched_transaction_config inifile section in
                                          (section, batched_transaction_cfg)::a
                                        else
                                          a)
                                     [] (inifile # sects) in
    let () = if List.length remaining > 0 then
        failwith ("Can't find config section for: " ^ (String.concat "," remaining))
    in
    let quorum_function = _get_quorum_function inifile in
    let lease_period = _get_lease_period inifile in
    let cluster_id = _get_cluster_id inifile in
    let m_n_cfg = get_nursery_cfg inifile config_file in
    let overwrite_tlog_entries = _tlog_entries_overwrite inifile in
    let max_value_size = _max_value_size inifile in
    let max_buffer_size = _max_buffer_size inifile in
    let client_buffer_capacity = _client_buffer_capacity inifile in
    let lcnum = _lcnum inifile in
    let ncnum = _ncnum inifile in
    let tls_ca_cert = _tls_ca_cert inifile in
    let tls_service = _tls_service inifile in
    let tls_service_validate_peer = _tls_service_validate_peer inifile in
    let tls_version = _tls_version inifile in
    let tls_cipher_list = _tls_cipher_list inifile in
    let tls = match tls_ca_cert with
      | None -> None
      | Some ca_cert ->
          let cfg = TLSConfig.Cluster.make
                      ~ca_cert
                      ~service:tls_service
                      ~service_validate_peer:tls_service_validate_peer
                      ~protocol:tls_version
                      ~cipher_list:tls_cipher_list
          in
          Some cfg
    in
    let cluster_cfg =
      { cfgs;
        log_cfgs;
        batched_transaction_cfgs;
        nursery_cfg = m_n_cfg;
        _master = fm;
        quorum_function;
        _lease_period = lease_period;
        cluster_id = cluster_id;
        plugins = plugin_names;
        overwrite_tlog_entries;
        max_value_size;
        max_buffer_size;
        client_buffer_capacity;
        lcnum;
        ncnum;
        tls;
      }
    in
    cluster_cfg


  let node_name t = t.node_name
  let home t = t.home

  let client_addresses t = (t.ips, t.client_port)

  let get_master t = t.master

  let get_node_cfgs_from_file () = read_config !config_file

  let test ccfg ~cluster_id = ccfg.cluster_id = cluster_id

  open Lwt
  let validate_dirs t =
    Logger.debug_ "Node_cfg.validate_dirs" >>= fun () ->
    if t.is_test then Lwt.return ()
    else
      begin
        let is_ok name =
          try
            let s = Unix.stat name in s.Unix.st_kind = Unix.S_DIR
          with _ -> false
        in
        let verify_exists dir msg exn =
          if not (is_ok dir)
          then
            begin
              Logger.fatal_f_ "%s '%s' doesn't exist, or insufficient permissions" msg dir >>= fun () ->
              Lwt.fail exn
            end
          else
            Lwt.return ()
        in

        let verify_writable dir msg exn =
          let handle_exn =
            (* Work-around: Logger macro doesn't accept an 'exn' argument not
             * called 'exn' *)
            let log exn =
              Logger.fatal_f_ ~exn "Failure while verifying %s '%s'" msg dir
            in
            function
              (* Set of exceptions we map to some specific exit code *)
              | Unix.Unix_error(Unix.EPERM, _, _)
              | Unix.Unix_error(Unix.EACCES, _, _)
              | Unix.Unix_error(Unix.EROFS, _, _)
              | Unix.Unix_error(Unix.ENOSPC, _, _) as e ->
                  log e >>= fun () ->
                  Lwt.fail exn
              | e ->
                  log e >>= fun () ->
                  Lwt.fail e
          in


          let safe_unlink fn =
            Lwt.catch
              (fun () -> File_system.unlink fn)
              handle_exn in

          let fn = Printf.sprintf "%s/touch-%d" dir (Unix.getpid ()) in

          let check () =
            safe_unlink fn >>= fun () ->
            let go () =
              Logger.debug_f_ "Touching %S" fn >>= fun () ->
              Lwt_unix.openfile fn
                [Lwt_unix.O_RDWR; Lwt_unix.O_CLOEXEC; Lwt_unix.O_CREAT; Lwt_unix.O_EXCL]
                0o600 >>= fun fd ->

              Lwt.finalize
                (fun () ->
                  Logger.debug_f_ "Write byte to %S" fn >>= fun () ->
                  Lwt_unix.write fd "0" 0 1 >>= fun _ ->
                  if t._fsync_tlog_dir
                  then
                    begin
                      Logger.debug_f_ "Fsync %S" fn >>= fun () ->
                      Lwt_unix.fsync fd
                    end
                  else
                    Lwt.return ())
                (fun () ->
                  Lwt_unix.close fd)
            in
            Lwt.catch go handle_exn
          in

          Lwt.finalize check (fun () -> safe_unlink fn)
        in

        let verify dir msg exn =
          verify_exists dir msg exn >>= fun () ->
          verify_writable dir msg exn
        in

        verify t.home "Home dir" (InvalidHomeDir t.home) >>= fun () ->
        verify t.tlog_dir "Tlog dir" (InvalidTlogDir t.tlog_dir) >>= fun () ->
        verify t.tlx_dir "Tlx dir" (InvalidTlxDir t.tlx_dir) >>= fun () ->
        verify t.head_dir "Head dir" (InvalidHeadDir t.head_dir)

      end
end
