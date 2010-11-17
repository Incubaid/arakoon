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


let default_lease_expiry = 60
let _COMPRESSION_DEFAULT = true;
module Node_cfg = struct
  type t = {node_name:string;
	    ip:string;
	    client_port:int;
	    messaging_port:int;
	    home:string;
	    log_dir:string;
	    log_level:string;
	    lease_period:int;
	    forced_master: string option;
	   }

  let make_test_config n_nodes forced_master lease_period = 
    let make_one n =
      let ns = (string_of_int n) in
      let home = "#MEM#t_arakoon_" ^ ns in
      {
	node_name = "t_arakoon_" ^ ns;
	ip = "127.0.0.1";
	client_port = (4000 + n);
	messaging_port = (4010 + n);
	home = home;
	log_dir = "none";
	log_level = "DEBUG";
	lease_period = lease_period;
	forced_master = forced_master;
      }
    in
    let rec loop acc = function
      | 0 -> acc
      | n -> let o = make_one (n-1) in
	     loop (o::acc) (n-1)
    in
    let cfgs = loop [] n_nodes in
    let quorum_function = Quorum.quorum_function in
    let lease_expiry = default_lease_expiry in
    (cfgs, forced_master, quorum_function, lease_expiry, false)

  let string_of (t:t) =
    let template =
      "{node_name=\"%s\"; ip=\"%s\"; client_port=%d; " ^^
	"messaging_port=%d; home=\"%s\"; log_dir=\"%s\"; log_level=\"%s\" }"
    in
      Printf.sprintf template
	t.node_name t.ip t.client_port t.messaging_port t.home
	t.log_dir
	t.log_level

  let tlog_file_name t =
    t.home ^ "/" ^ t.node_name ^ ".tlog"

  let read_config config_file =
    let node_names inifile = 
      let nodes_s = inifile # getval "global" "nodes" in
      let nodes = Str.split (Str.regexp "[, \t]+") nodes_s in
      nodes
    in
    let compression inifile = 
      try 
	let c_s = inifile # getval "global" "compression" in
	Scanf.sscanf c_s "%b" (fun b -> b) 
      with (Inifiles.Invalid_element e) -> _COMPRESSION_DEFAULT
    in
    let lease_expiry inifile =
      try
	let les = (inifile # getval "global" "lease_expiry") in
	Scanf.sscanf les "%i" (fun i -> i)
      with (Inifiles.Invalid_element _) -> default_lease_expiry
    in
    let forced_master inifile =
      try
	let m = (inifile # getval "global" "master") in
	let nodes = node_names inifile in
	if not (List.mem m nodes)
	then
	  failwith (Printf.sprintf "'%s' needs to have a config section [%s]" m m)
        else Some m
      with (Inifiles.Invalid_element _) -> None
    in
    let node_config inifile node_section fm =
      let get_string x =
	try
	  let strip s = Scanf.sscanf s "%s" (fun i -> i) in
	  let with_spaces= inifile # getval node_section x in
	  strip with_spaces
	with (Inifiles.Invalid_element e) ->
	  failwith (Printf.sprintf "'%s' was missing from %s"
		      e config_file)
      in
      let get_int x = Scanf.sscanf (get_string x) "%i" (fun i -> i) in

      let node_name = node_section in
      let ip = get_string "ip" in
      let client_port = get_int "client_port" in
      let messaging_port = get_int "messaging_port" in
      let home = get_string "home" in
      let log_level = String.lowercase (get_string "log_level") in
      let lease_period = lease_expiry inifile in
      let log_dir = get_string "log_dir" in
	{node_name=node_name;
	 ip=ip;
	 client_port=client_port;
	 messaging_port= messaging_port;
	 home=home;
	 log_dir = log_dir;
	 log_level = log_level;
	 lease_period = lease_period;
	 forced_master = fm;
	}
    in
    let inifile = new Inifiles.inifile config_file in
    let fm = forced_master inifile in
    let nodes = node_names inifile in
    let n_nodes = List.length nodes in
    let cfgs, remaining = List.fold_left
      (fun (a,remaining) section ->
	if List.mem section nodes
	then
	  let cfg = node_config inifile section fm in
	  let new_remaining = List.filter (fun x -> x <> section) remaining in
	  (cfg::a, new_remaining)
	else (a,remaining))
      ([],nodes) (inifile # sects) in
    let () = if List.length remaining > 0 then
	failwith ("Can't find config section for: " ^ (String.concat "," remaining))
    in
    
    
    let quorum_function =
      try
	let qs = (inifile # getval "global" "quorum") in
	let qi = Scanf.sscanf qs "%i" (fun i -> i) in
	if 1 <= qi & qi <= n_nodes then
	  fun n -> qi
	else
	  let msg = Printf.sprintf "fixed quorum should be 1 <= %i <= %i"
	    qi n_nodes in
	  failwith msg
      with (Inifiles.Invalid_element _) -> Quorum.quorum_function
    in
    let lease_period = lease_expiry inifile in
    let compressed = compression inifile in
    cfgs, fm, quorum_function, lease_period, compressed


  let node_name t = t.node_name
  let home t = t.home

  let client_address t = Network.make_address t.ip t.client_port
  
  let forced_master t = t.forced_master

end
