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


let config_file = ref "cfg/arakoon.ini"

let default_lease_period = 10
let default_max_value_size = 8 * 1024 * 1024
let default_max_buffer_size = 32 * 1024 * 1024
let default_client_buffer_capacity = 32

open Master_type
open Client_cfg
open Log_extra

exception InvalidHomeDir of string
exception InvalidTlogDir of string

module Node_cfg = struct

  type t = {node_name:string;
        ips: string list;
        client_port:int;
        messaging_port:int;
        home:string;
        tlog_dir:string;
        log_dir:string;
        log_level:string;
        log_config:string option;
        batched_transaction_config:string option;
        lease_period:int;
        master: master;
        is_laggy : bool;
        is_learner : bool;
        targets : string list;
        use_compression : bool;
        is_test : bool;
        reporting: int;
       }

  let _so2s = Log_extra.string_option2s

  let string_of (t:t) =
    let template =
      "{node_name=%S; ips=%s; client_port=%d; " ^^
        "messaging_port=%d; home=%S; tlog_dir=%S; " ^^
        "log_dir=%S; log_level:%S; log_config=%s; " ^^
        "batched_transaction_config=%s; lease_period=%i; " ^^
        "master=%S; is_laggy=%b; is_learner=%b; " ^^
        "targets=%s; use_compression=%b; is_test=%b; " ^^
        "reporting=%i; " ^^
        "}"
    in
    Printf.sprintf template
      t.node_name
      (list2s (fun s -> s) t.ips)
      t.client_port
      t.messaging_port t.home t.tlog_dir
      t.log_dir t.log_level (_so2s t.log_config)
      (_so2s t.batched_transaction_config) t.lease_period
      (master2s t.master) t.is_laggy t.is_learner
      (list2s (fun s -> s) t.targets) t.use_compression t.is_test
      t.reporting

  type log_cfg =
      {
        client_protocol: string option;
        paxos: string option;
        tcp_messaging: string option;
      }

  let string_of_log_cfg lcfg =
    Printf.sprintf "{ client_protocol=%s; paxos=%s; tcp_messaging=%s }"
      (_so2s lcfg.client_protocol) (_so2s lcfg.paxos) (_so2s lcfg.tcp_messaging)

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
    Printf.sprintf "{ max_entries=%s; max_size=%s }"
      (Log_extra.int_option2s btc.max_entries)
      (Log_extra.int_option2s btc.max_size)

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
    }

  let string_of_cluster_cfg cluster_cfg =
    let buffer = Buffer.create 200 in
    Buffer.add_string buffer "{ node_cfgs=[ ";
    List.iter (fun cfg -> Buffer.add_string buffer (string_of cfg); Buffer.add_string buffer ",") cluster_cfg.cfgs;
    List.iter (fun (name,lcfg) ->
      Buffer.add_string buffer ("]; log_cfg['" ^ name ^ "']=");
      Buffer.add_string buffer (string_of_log_cfg lcfg)) cluster_cfg.log_cfgs;
    List.iter (fun (name,btcfg) ->
      Buffer.add_string buffer ("; batched_transaction_cfg['" ^ name ^ "']=");
      Buffer.add_string buffer (string_of_btc btcfg)) cluster_cfg.batched_transaction_cfgs;
    Buffer.add_string buffer "; nursery_cfg=";
    Buffer.add_string buffer (Log_extra.option2s (fun (s,ncfg) -> s ^ "," ^ ClientCfg.to_string ncfg) cluster_cfg.nursery_cfg);
    Buffer.add_string buffer
      (Printf.sprintf
         ("; _master=%s; _lease_period=%i; cluster_id=%s; plugins=%s; "
          ^^ "overwrite_tlog_entries=%s; max_value_size=%i; max_buffer_size=%i; "
          ^^ "client_buffer_capacity=%i }")
         (master2s cluster_cfg._master) cluster_cfg._lease_period cluster_cfg.cluster_id
         (List.fold_left (^) "" cluster_cfg.plugins)
         (Log_extra.int_option2s cluster_cfg.overwrite_tlog_entries)
         cluster_cfg.max_value_size cluster_cfg.max_buffer_size
         cluster_cfg.client_buffer_capacity);
    Buffer.contents buffer

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
        log_dir = ":None";
        log_level = "DEBUG";
        log_config = Some "default_log_config";
        batched_transaction_config = Some "default_batched_transaction_config";
        lease_period = lease_period;
        master = master;
        is_laggy = false;
        is_learner = false;
        targets = [];
        use_compression = true;
        is_test = true;
        reporting = 300;
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
    }
    in
    cluster_cfg
    



  let tlog_dir t = t.tlog_dir 
  
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
    with ex -> 
      None 

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
      if 1 <= qi & qi <= n_nodes 
      then fun n -> qi
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
    let log_level = String.lowercase (get_string "log_level")  in
    let log_config = Ini.get inifile node_name "log_config" (Ini.p_option Ini.p_string) (Ini.default None) in
    let batched_transaction_config = Ini.get inifile node_name "batched_transaction_config" (Ini.p_option Ini.p_string) (Ini.default None) in
    let is_laggy = get_bool "laggy" in
    let is_learner = get_bool "learner" in
    let use_compression = not (get_bool "disable_tlog_compression") in
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
    {node_name;
     ips;
     client_port;
     messaging_port;
     home;
     tlog_dir;
     log_dir;
     log_level;
     log_config;
     batched_transaction_config;
     lease_period;
     master;
     is_laggy;
     is_learner;
     targets;
     use_compression;
     is_test = false;
     reporting;
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
    Lwt_log.debug "Node_cfg.validate_dirs" >>= fun () ->
    if t.is_test then Lwt.return ()
    else
      begin
	let is_ok name = 
	  try
	    let s = Unix.stat name in s.Unix.st_kind = Unix.S_DIR
	  with _ -> false
	in
        if not (is_ok t.home)
        then
          Lwt_log.fatal_f
            "Home dir '%s' doesn't exist, or insufficient permissions"
            t.home >>= fun () ->
          Lwt.fail (InvalidHomeDir t.home)
        else
        if not (is_ok t.tlog_dir)
        then
          Lwt_log.fatal_f
            "Tlog dir '%s' doesn't exist, or insufficient permissions"
            t.tlog_dir >>= fun () ->
          Lwt.fail (InvalidTlogDir t.tlog_dir)
        else
        Lwt.return ()
      end 
end
