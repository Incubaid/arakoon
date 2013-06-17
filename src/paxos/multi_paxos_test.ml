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

open Mp_msg
open Lwt
open MPMessage
open Messaging
open Tcp_messaging
open Extra
open Multi_paxos
open Update
open Lwt_buffer
open Master_type

module S = (val (Store.make_store_module (module Mem_store)))

let sn2s = Sn.string_of 

let test_take () = Lwt.return (None, (fun s -> Lwt.return ()))

let build_name j = "c" ^ (string_of_int j) 

let build_names n =
  let rec _loop names = function
    | 0 -> names
    | j -> _loop (build_name j :: names ) (j-1)
  in _loop [] n

let on_witness who i = () 

let get_value tlog_coll i = tlog_coll # get_last_value i 

let test_generic network_factory n_nodes () =
  Logger.info_ "START:TEST_GENERIC" >>= fun () ->
  let get_buffer, send, nw_run, nw_register = network_factory () in
  let current_n = 42L
  and current_i = 0L in
  let values = Hashtbl.create 10 in
  let on_accept me (v,n,i) = Logger.debug_f_ "%s: on_accept(%s,%s)" me (sn2s n) (sn2s i) 
  in
  let on_consensus me (v,n,i) =
    let () = Hashtbl.add values me v in
    Logger.debug_f_ "%s: on_consensus(%s,%s)" me (sn2s n) (sn2s i) >>= fun () -> 
    Lwt.return [Store.Ok None]
  in
  let last_witnessed who = Sn.of_int (-1000) in
  let inject_buffer = Lwt_buffer.create_fixed_capacity 1 in
  let inject_ev q e = Lwt_buffer.add e q in
  S.make_store "MEM#store" >>= fun store ->
  Mem_tlogcollection.make_mem_tlog_collection "MEM#tlog" true "???">>= fun tlog_coll ->
  let base = {me = "???";
	          others = [] ;
	          is_learner = false;
	          send = send;
	          get_value = get_value tlog_coll;
	          on_accept= on_accept "???";
              on_consensus = on_consensus "???";
              on_witness = on_witness;
	          last_witnessed = last_witnessed;
	          quorum_function = Multi_paxos.quorum_function;
	          master=Elected;
	          store = store;
              store_module = (module S);
	          tlog_coll = tlog_coll;
	          other_cfgs = [];
	          lease_expiration = 60;
	          inject_event = inject_ev inject_buffer;
	          cluster_id = "whatever";
              quiesced = false;
	         }
  in
  let all_happy = build_names (n_nodes -1) in
  let build_others name = List.filter (fun n -> n <> name) all_happy in
  let steps = 100 * n_nodes in
  let build_n i=
    let rec _loop ts = function
      | x when x < 0 -> failwith ">=1"
      | 0 -> ts
      | i -> let me = build_name i in
	     let others = "c0" :: build_others me in
	     let inject_buffer = Lwt_buffer.create_fixed_capacity 1 in
	     let constants = { base with
	       me = me;
	       others = others;
	       on_accept = on_accept me;
	       on_consensus = on_consensus me;
	       inject_event = inject_ev inject_buffer;
	     } in
	     let t =
	       let expected prev_key key =
		     Logger.debug_f_ "%s: node from %s to %s" me (Multi_paxos_type.show_transition prev_key) 
		       (Multi_paxos_type.show_transition key) >>= fun () ->
		     match key with
		       | (Multi_paxos_type.Slave_steady_state x) -> Lwt.return (Some x)
		       | _ -> Lwt.return None
	       in
	       let client_buffer = Lwt_buffer.create () in
	       let inject_buffer = Lwt_buffer.create_fixed_capacity 1 in
	       let election_timeout_buffer = Lwt_buffer.create_fixed_capacity 1 in
	       let buffers = Multi_paxos_fsm.make_buffers 
		     (client_buffer,get_buffer me, 
		      inject_buffer, election_timeout_buffer) in
	       Multi_paxos_fsm.expect_run_forced_slave 
		     constants buffers expected steps (current_i,Sn.start)
		   >>= fun result ->
	       Logger.debug_f_ "%s: node done." me >>= fun () ->
	       Lwt.return ()
	     in
	     _loop (t :: ts) (i-1)
    in _loop [] i
  in
  let ts = build_n (n_nodes -1) in
  let me = "c0" in
  Logger.debug_f_ "%s: %d other nodes started" me (List.length ts) >>= fun () ->
  let constants = { base with 
      me = me;
      others = all_happy;
      on_accept = on_accept me;
      on_consensus = on_consensus me;
      inject_event = inject_ev inject_buffer;
  } in
  let c0_t () =
    let expected prev_key key =
      Logger.debug_f_ "%s: c0 from %s to %s" me (Multi_paxos_type.show_transition prev_key) 
	(Multi_paxos_type.show_transition key) >>= fun () ->
      match key with
	| (Multi_paxos_type.Stable_master x) -> Lwt.return (Some x)
	| _ -> Lwt.return None
    in
    let inject_buffer = Lwt_buffer.create () in
    let election_timeout_buffer = Lwt_buffer.create () in
    let client_buffer = Lwt_buffer.create () in
    let buffers = Multi_paxos_fsm.make_buffers 
      (client_buffer, 
       get_buffer me, 
       inject_buffer, 
       election_timeout_buffer) in
    Multi_paxos_fsm.expect_run_forced_master constants buffers 
      expected steps current_n current_i
    >>= fun (v', n, i,_) ->
    Logger.debug_f_ "%s: consensus reached on n:%s" me (Sn.string_of n)
  in
  let addresses = List.map (fun name -> name , ("127.0.0.1", 7777)) 
    ("c0"::all_happy) in
  let () = nw_register addresses in
  (* wrap with first a yield cause else the situation blocks; 
     a callback when the server is ready could perhaps help 
  *)
  let nw_run_wrap () = Lwt_unix.yield () >>= nw_run in
  Lwt.pick [
    (Lwt.join ( (c0_t ()) :: ts) >>= fun () -> Logger.debug_f_ "%s: after join" me);
    (nw_run_wrap () )] >>= fun () ->
  let len = Hashtbl.length values in
  Logger.debug_f_ "%s: end of main... validating len = %d" me len >>= fun () ->
  let all_consensusses = Hashtbl.fold 
    (fun a b acc -> 
      let bs = Value.value2s b in
      (a,bs) :: acc) values [] 
  in
  Lwt_list.iter_s 
    (fun (name, update_string) -> 
      Logger.debug_f_ "%s:%s"  name update_string) 
    all_consensusses
  >>= fun () ->
  let _ = 
    List.fold_left (fun maybe_ms (name,us) -> 
      match maybe_ms with 
	    | None -> Some us 
	    | Some ms ->
	        let msg = Printf.sprintf "%s:consensus" name in
	        Extra.eq_conv (fun s -> s) msg ms us;
	        maybe_ms
    ) 
      None all_consensusses
  in
  Extra.eq_int "values in tbl" 1 (Hashtbl.length values);
  Lwt.return ()


let test_master_loop network_factory ()  =
  let get_buffer, send, nw_run, nw_register = 
    network_factory () in
  let me = "c0" in
  let i0 = 0L in
  let others = [] in
  let rec create_updates acc = function
    | 0 -> acc
    | n ->
        let key = Printf.sprintf "key_%d" n in
        let value = Printf.sprintf "value_%d" n in
        let update = Update.Set( key, value ) in
        create_updates (update :: acc) (n-1)
  in
  let updates = create_updates [] 5 in
  let finished = fun (a:Store.update_result) ->
    Logger.debug_f_ "%s: finished" me >>= fun () ->
    Lwt.return ()
  in
  let client_buffer = Lwt_buffer.create () in
  let () = Lwt_extra.ignore_result (
    Lwt_list.iter_s
      (fun x -> 
        Lwt_buffer.add (x, finished) client_buffer >>= fun () ->
	    Lwt_unix.sleep 2.0
      ) updates
  ) in
  let on_consensus (_,n,i) =
    Logger.debug_f_ "%s: consensus: n:%s i:%s" me (sn2s n) (sn2s i) >>= fun () ->
    Lwt.return [Store.Ok None]
  in
  let on_accept (v,n,i) = Logger.debug_f_ "%s: accepted n:%s i:%s" me (sn2s n) (sn2s i) 
  in
  let last_witnessed who = Sn.of_int (-1000) in
  let inject_buffer = Lwt_buffer.create () in
  let election_timeout_buffer = Lwt_buffer.create() in
  let inject_event e = Lwt_buffer.add e inject_buffer in

  S.make_store "MEM#store" >>= fun store ->
  Mem_tlogcollection.make_mem_tlog_collection "MEM#tlog" true me >>= fun tlog_coll ->
  let constants = {me = me; 
		   is_learner = false;
		   others = others;
		   send = send;
		   get_value = get_value tlog_coll;
		   on_accept = on_accept;
		   on_consensus = on_consensus;
		   on_witness = on_witness;
		   last_witnessed = last_witnessed;
		   quorum_function = Multi_paxos.quorum_function;
		   master = Elected;
		   store = store;
           store_module = (module S);
		   tlog_coll = tlog_coll;
		   other_cfgs = [];
		   lease_expiration = 60;
		   inject_event = inject_event;
		   cluster_id = "whatever";
		   quiesced = false;
		  } in
  let continue = ref 2 in
  let c0_t () =
    let expected prev_key key =
      Logger.debug_f_ "%s: c0 from %s to %s" me (Multi_paxos_type.show_transition prev_key) 
	(Multi_paxos_type.show_transition key) >>= fun () ->
      match key with
	    | (Multi_paxos_type.Stable_master x) ->
	        if !continue = 0 
            then Lwt.return (Some x) 
            else
	          let () = continue := (!continue -1) in 
              Lwt.return None
	    | _ -> Lwt.return None
    in
    let current_n = Sn.start in
    let buffers = 
      Multi_paxos_fsm.make_buffers 
	    (client_buffer, 
	     get_buffer me, 
	     inject_buffer, 
	     election_timeout_buffer) in
    Multi_paxos_fsm.expect_run_forced_master constants buffers expected 20 current_n i0
    >>= fun result -> Logger.debug_f_ "%s: after loop" me
  in
  Lwt.pick [ c0_t ();]

type 'a simulation =
    {mutable scenario: (MPMessage.t * string * string) list;
     waiters : (string,'a) Hashtbl.t}

let build_perfect () =
  let qs = Hashtbl.create 7 in
  let get_q target =
    if Hashtbl.mem qs target then Hashtbl.find qs target
    else
      let q = Lwt_buffer.create () in
      let () = Hashtbl.add qs target q in
      q
  in
  let send msg source (target:string) =
    Logger.debug_f_ "%s: %s --- %s ----->? %s" source source (string_of msg) target >>= fun () ->
    let q = get_q target in
    Lwt_buffer.add (Mp_msg.MPMessage.generic_of msg,source) q >>= fun () ->
    Logger.debug_f_ "%s: added to buffer of %s" source target
  in
  let get_buffer = get_q in
  let run () = Lwt_unix.sleep 2.0 in
  let register (xs:(string * (string * int)) list) = () in
  get_buffer, send, run, register


let build_tcp () =
  let (m : messaging) = new tcp_messaging (["127.0.0.1"], 7777) "yummie"
    (fun _ _ _ -> false) Node_cfg.default_max_buffer_size
  in
  let network = network_of_messaging m in
  network




let test_simulation filters () =
  Random.init 42;

   let me = "c0" in
  let current_n = 42L in
  let current_i = 0L in
  let on_accept me (v,n,i) = Logger.debug_f_ "%s: on_accept: (%s,%s)" me (sn2s n) (sn2s i) in
  let on_consensus me (v,n,i) =
    Logger.debug_f_ "%s: on_consensus: (%s,%s) " me (sn2s n) (sn2s i)
    >>= fun () ->
    Lwt.return [Store.Ok None]
  in
  let last_witnessed who = Sn.of_int (-1000) in
  let inject_buffer = Lwt_buffer.create () in
  let election_timeout_buffer = Lwt_buffer.create () in
  let buffers = Hashtbl.create 7 in
  let () = Hashtbl.add buffers "c0" (Lwt_buffer.create ()) in
  let () = Hashtbl.add buffers "c1" (Lwt_buffer.create ()) in
  let () = Hashtbl.add buffers "c2" (Lwt_buffer.create ()) in
  let client_buffer = Lwt_buffer.create() in
  let get_buffer who = Hashtbl.find buffers who in
  let inject_event e = Lwt_buffer.add e inject_buffer in
  let send msg source target =
    let msg_s = string_of msg in
    Logger.debug_f_ "%s: sends %s to %s" me msg_s  target >>= fun () ->
    let ok = List.fold_left (fun acc f -> acc && f (msg,source,target)) true filters in
    if ok then
      begin
	let b = get_buffer target in
	let gm = Mp_msg.MPMessage.generic_of msg in
	Lwt_buffer.add (gm,source) b>>= fun () ->
	Lwt.return ()
      end
    else
      Logger.debug_f_ "got (%s,%s,%s) => dropping" msg_s source target
  in
  
  S.make_store "MEM#store"  >>= fun store ->
  Mem_tlogcollection.make_mem_tlog_collection "MEM#tlog" true me >>= fun tlog_coll ->
  let constants = {
    me = me;
	is_learner = false;
	others = ["c1";"c2"];
	send = send;
	get_value = get_value tlog_coll;
	on_accept = on_accept me;
	on_consensus = on_consensus me;
	on_witness = on_witness;
	last_witnessed = last_witnessed;
	quorum_function = Multi_paxos.quorum_function;
	master = Elected;
	store = store;
    store_module = (module S);
	tlog_coll = tlog_coll;
	other_cfgs = [];
	lease_expiration = 60;
	inject_event = inject_event;
	cluster_id = "whatever";
    quiesced = false;
  } in
  let c0_t () =
    let expected prev_key key =
      Logger.debug_f_ "%s: c0 from %s to %s" me (Multi_paxos_type.show_transition prev_key) 
	(Multi_paxos_type.show_transition key) >>= fun () ->
      match key with
	| (Multi_paxos_type.Stable_master x) -> Lwt.return (Some x)
	| _ -> Lwt.return None
    in
    let buffers = Multi_paxos_fsm.make_buffers
      (client_buffer, 
       get_buffer me, 
       inject_buffer, 
       election_timeout_buffer) in
    Multi_paxos_fsm.expect_run_forced_master constants buffers 
      expected 50 current_n current_i
    >>= fun (v', n, i, _) ->
    Logger.debug_f_ "%s: consensus reached: (%s,%s)" me (sn2s n) (sn2s i)
  in
  let cx_t me other =
    let inject_buffer = Lwt_buffer.create () in
    let election_timeout_buffer = Lwt_buffer.create () in
    let client_buffer = Lwt_buffer.create () in
    let constants =
      {constants with me=me;
	others = ["c0"; other];
	on_accept = on_accept me;
	on_consensus = on_consensus me;
	inject_event = (fun e -> Lwt_buffer.add e inject_buffer);
      }
    in
    let expected prev_key key =
      Logger.debug_f_ "%s: node from %s to %s" me 
	    (Multi_paxos_type.show_transition prev_key) 
	    (Multi_paxos_type.show_transition key) >>= fun () ->
      match key with
	    | (Multi_paxos_type.Slave_steady_state x) -> Lwt.return (Some x)
	    | _ -> Lwt.return None
    in
    let buffers = Multi_paxos_fsm.make_buffers
      (client_buffer, 
       get_buffer me, 
       inject_buffer, 
       election_timeout_buffer) in
    Multi_paxos_fsm.expect_run_forced_slave constants buffers expected 50 (current_i,Sn.start)
    >>= fun result ->
    Logger.debug_f_ "%s: node done." me >>= fun () ->
    Lwt.return ()
  in
  Lwt.pick [c0_t (); 
	        cx_t "c1" "c2"; 
	        cx_t "c2" "c1"; 
	        begin
	          Lwt_unix.sleep 80.0 >>= fun () -> 
	          Llio.lwt_failfmt "test: should have finished successfully by now";
	        end
	       ] >>= fun () ->
  Logger.debug_f_ "%s: after pick" me


let ideal    = [ (fun (msg,s,t) -> true) ]
let c2_fails = [ (fun (msg,s,t) -> s <> "c2")]


let xtodo () =
  OUnit.todo "re-enable"

(* Lwt_extra.run (test ideal);; *)
open OUnit
let w = lwt_test_wrap
let suite = "basic" >::: [
  "singleton_perfect" >:: w (test_generic build_perfect 1);
  "pair_perfect"  >:: w (test_generic build_perfect 2);
  (*"trio"          >:: w (test_generic build_perfect 3);
  "quartet"       >:: w (test_generic build_perfect 4);
  "quintet"       >:: w (test_generic build_perfect 5);
  "sextet"        >:: w (test_generic build_perfect 6);
  "ideal_simulation" >:: w (test_simulation ideal);
  "c2_fails"      >:: w (test_simulation c2_fails);*)
  (*
  "c2_fails"      >:: w (test_simulation c2_fails);
  "c1_nak"        >:: w (test_simulation c1_nak);
  *)
  (* "c1_nak"        >:: xtodo;
  "pair_tcp"      >:: xtodo; *)
  (* "pair_tcp"      >:: w (test_generic build_tcp 2); *)
  "master_loop_1" >:: w (test_master_loop build_perfect);
];;



