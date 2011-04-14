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
open Master_type

module Node_cfg = struct

  type t = {node_name:string;
	    ip:string;
	    client_port:int;
	    messaging_port:int;
	    home:string;
	    tlog_dir:string;
	    log_dir:string;
	    log_level:string;
	    lease_period:int;
	    master: master;
	    is_laggy : bool;
	    is_learner : bool;
	    targets : string list;
	   }

  type cluster_cfg = 
      { cfgs: t list;
	_master: master;
	quorum_function: int -> int;
	_lease_period: int;
	cluster_id : string;
	plugins: string list;
      }

  let make_test_config n_nodes master lease_period = 
    let make_one n =
      let ns = (string_of_int n) in
      let home = "_build/src/plugins" in (* TODO: Turkish *)
      {
	node_name = "t_arakoon_" ^ ns;
	ip = "127.0.0.1";
	client_port = (4000 + n);
	messaging_port = (4010 + n);
	home = home;
	tlog_dir = home;
	log_dir = "none";
	log_level = "DEBUG";
	lease_period = lease_period;
	master = master;
	is_laggy = false;
	is_learner = false;
	targets = [];
      }
    in
    let rec loop acc = function
      | 0 -> acc
      | n -> let o = make_one (n-1) in
	     loop (o::acc) (n-1)
    in
    let cfgs = loop [] n_nodes in
    let quorum_function = Quorum.quorum_function in
    let lease_period = default_lease_period in
    let cluster_id = "ricky" in
    let cluster_cfg = { cfgs= cfgs; 
			_master = master;
			quorum_function = quorum_function;
			_lease_period = lease_period;
			cluster_id = cluster_id;
			plugins = ["plugin_update_max"];
		      }
    in
    cluster_cfg
    

  let string_of (t:t) =
    let template =
      "{node_name=\"%s\"; ip=\"%s\"; client_port=%d; " ^^
	"messaging_port=%d; home=\"%s\"; tlog_dir=\"%s\"; " ^^ 
	"log_dir=\"%s\"; log_level=\"%s\"; laggy=%b; is_learner=%b}"
    in
      Printf.sprintf template
	t.node_name t.ip t.client_port t.messaging_port 
	t.home
	t.tlog_dir
	t.log_dir
	t.log_level
	t.is_laggy
	t.is_learner

  let tlog_dir t = t.tlog_dir 
  let tlog_file_name t =
    t.home ^ "/" ^ t.node_name ^ ".tlog"

  let _get_string_list inifile section name =
    let nodes_s = inifile # getval section name in
    let nodes = Str.split (Str.regexp "[, \t]+") nodes_s in
    nodes

  let _node_names inifile = _get_string_list inifile "global" "cluster"

  let _plugins inifile = 
    try
      _get_string_list inifile "global" "plugins"
    with Inifiles.Invalid_element _ -> []

  let _get_lease_period inifile =
    try
      let les = (inifile # getval "global" "lease_period") in
      Scanf.sscanf les "%i" (fun i -> i)
    with (Inifiles.Invalid_element _) -> default_lease_period

  let _get_bool inifile node_section x = 
      try Scanf.sscanf (inifile # getval node_section x) "%b" (fun b -> b) 
      with (Inifiles.Invalid_element _) -> false

  let _forced_master inifile =
    let master =
      try
	let m = (inifile # getval "global" "master") in
	let nodes = _node_names inifile in
	if not (List.mem m nodes)
	then
	  failwith (Printf.sprintf "'%s' needs to have a config section [%s]" m m)
	else 
	  if _get_bool inifile "global" "preferred_master" 
	  then (Preferred m)
	  else (Forced m)
      with (Inifiles.Invalid_element _) -> Elected
    in
    master

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




  let _node_config inifile node_name master =
    let get_string x =
      try
	let strip s = Scanf.sscanf s "%s" (fun i -> i) in
	let with_spaces= inifile # getval node_name x in
	strip with_spaces
      with (Inifiles.Invalid_element e) ->
	failwith (Printf.sprintf "'%s' was missing from node: %s" e node_name)
    in
    let get_bool x = _get_bool inifile node_name x in
    let get_int x = Scanf.sscanf (get_string x) "%i" (fun i -> i) in
    let ip = get_string "ip" in
    let client_port = get_int "client_port" in
    let messaging_port = get_int "messaging_port" in
    let home = get_string "home" in
    let tlog_dir = try get_string "tlog_dir" with _ -> home in
    let log_level = String.lowercase (get_string "log_level")  in
    let is_laggy = get_bool "laggy" in
    let is_learner = get_bool "learner" in
    let targets = 
      if is_learner 
      then _get_string_list inifile node_name "targets"
      else []
    in
    let lease_period = _get_lease_period inifile in
    let log_dir = get_string "log_dir" in
    {node_name=node_name;
     ip=ip;
     client_port=client_port;
     messaging_port= messaging_port;
     home=home;
     tlog_dir = tlog_dir;
     log_dir = log_dir;
     log_level = log_level;
     lease_period = lease_period;
     master = master;
     is_laggy = is_laggy;
     is_learner = is_learner;
     targets = targets;
    }


  let read_config config_file =
    let inifile = new Inifiles.inifile config_file in
    let fm = _forced_master inifile in
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
    let () = if List.length remaining > 0 then
	failwith ("Can't find config section for: " ^ (String.concat "," remaining))
    in  
    
    let quorum_function = _get_quorum_function inifile in
    let lease_period = _get_lease_period inifile in
    let cluster_id = _get_cluster_id inifile in
    let cluster_cfg = 
      { cfgs = cfgs;
	_master = fm;
	quorum_function = quorum_function;
	_lease_period = lease_period;
	cluster_id = cluster_id;
	plugins = plugin_names;
      }
    in
    cluster_cfg


  let node_name t = t.node_name
  let home t = t.home

  let client_address t = Network.make_address t.ip t.client_port
  
  let get_master t = t.master

  let get_node_cfgs_from_file () = read_config !config_file 

  let test ccfg ~cluster_id = ccfg.cluster_id = cluster_id


end
