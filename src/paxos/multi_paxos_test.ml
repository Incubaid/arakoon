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

let sn2s = Sn.string_of 

let test_take () =
  Lwt.return (None, (fun s -> Lwt.return ()))

let build_name j = "c" ^ (string_of_int j) 

let build_names n =
  let rec _loop names = function
    | 0 -> names
    | j -> _loop (build_name j :: names ) (j-1)
  in _loop [] n


let test_generic network_factory n_nodes () =
  Lwt_log.info "START:TEST_GENERIC" >>= fun () ->
  let get_buffer, send, receive, peek, nw_run, nw_register = network_factory () in
  let current_n = 42L
  and current_i = 666L in
  let values = Hashtbl.create 10 in
  let on_accept me (v,n,i) =
    log ~me "on_accept(%s,%s)" (sn2s n) (sn2s i)
  in
  let on_consensus me (v,n,i) =
    let () = Hashtbl.add values me v in
    log ~me "on_consensus(%s,%s)" (sn2s n) (sn2s i)
      >>= fun () -> Lwt.return (Store.Ok None)
  in
  let on_witness who i = Lwt.return () in
  let get_value i = Lwt.fail (Failure "no_value") in

  let inject_buffer = Lwt_buffer.create_fixed_capacity 1 in
  let inject_ev q e = Lwt_buffer.add e q in
  Mem_store.make_mem_store "MEM#store" >>= fun store ->
  Mem_tlogcollection.make_mem_tlog_collection "MEM#tlog" >>= fun tlog_coll ->
  let base = {me = "???";
	      others = [] ;
	      send = send;
	      receive = receive;
	    
	      get_value = get_value;
	      on_accept= on_accept "???";
	      on_consensus = on_consensus "???";
	      on_witness = on_witness;
	      quorum_function = Multi_paxos.quorum_function;
	      forced_master=None;
	      store = store;
	      tlog_coll = tlog_coll;
	      other_cfgs = [];
	      lease_expiration = 60;
	      inject_event = inject_ev inject_buffer;
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
		 log ~me "node from %s to %s" (Multi_paxos_type.show_transition prev_key) 
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
		 constants buffers expected steps current_i
		 >>= fun result ->
	       log ~me "node done." >>= fun () ->
	       Lwt.return ()
	     in
	     _loop (t :: ts) (i-1)
    in _loop [] i
  in
  let ts = build_n (n_nodes -1) in
  let me = "c0" in
  log ~me "%d other nodes started" (List.length ts) >>= fun () ->
  let constants = { base with 
      me = me;
      others = all_happy;
      on_accept = on_accept me;
      on_consensus = on_consensus me;
      inject_event = inject_ev inject_buffer;
  } in
  let c0_t () =
    let expected prev_key key =
      log ~me "c0 from %s to %s" (Multi_paxos_type.show_transition prev_key) 
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
    >>= fun (v', n, i) ->
    log "consensus reached on n:%s" (Sn.string_of n)
  in
  let addresses = List.map (fun name -> name , ("127.0.0.1", 7777)) 
    ("c0"::all_happy) in
  let () = nw_register addresses in
  (* wrap with first a yield cause else the situation blocks; 
     a callback when the server is ready could perhaps help 
  *)
  let nw_run_wrap () = Lwt_unix.yield () >>= nw_run in
  Lwt.pick [
    (Lwt.join ( (c0_t ()) :: ts) >>= fun () -> log "after join");
    (nw_run_wrap () )] >>= fun () ->
  let len = Hashtbl.length values in
  log "end of main... validating len = %d" len >>= fun () ->
  let all_consensusses = Hashtbl.fold (fun a b acc -> 
    let Value.V us = b in
    let update,_ = Update.from_buffer us 0 in
    let d = Update.string_of update in
    (a,d) :: acc) values [] in
  Lwt_list.iter_s 
    (fun (name, update_string) -> 
      Lwt_log.debug_f "%s:%s"  name update_string) 
    all_consensusses
  >>= fun () ->
  List.fold_left (fun maybe_ms (name,us) -> 
    match maybe_ms with 
      | None -> Some us 
      | Some ms ->
	let msg = Printf.sprintf "%s:consensus" name in
	Extra.eq_conv (fun s -> s) msg ms us;
	maybe_ms
  ) 
    None all_consensusses;
  Extra.eq_int "values in tbl" n_nodes (Hashtbl.length values);
  Lwt.return ()


let test_master_loop network_factory ()  =
  let get_buffer, send, receive, peek, nw_run, nw_register = 
    network_factory () in
  let me = "c0" in
  let i0 = 666L in
  let others = [] in
  let values = ["1";"2";"3";"4";"5"] in
  let finished = fun (a:Store.update_result) ->
    log "finished" >>= fun () ->
    Lwt.return ()
  in
  let client_buffer = Lwt_buffer.create () in
  let () = Lwt.ignore_result (
    Lwt_list.iter_s
      (fun x ->
	Lwt_buffer.add (Some (Value.create x),finished) client_buffer 
	>>= fun () ->
	Lwt_unix.sleep 2.0
      ) values
  ) in
  let on_consensus (v,n,i) =
    log "consensus: n:%s i:%s" (sn2s n) (sn2s i) >>= fun () ->
    match v with
      | Value.V s -> Lwt.return (Store.Ok None)
  in
  let on_accept (v,n,i) =
    log "accepted n:%s i:%s" (sn2s n) (sn2s i) >>= fun () ->
    Lwt.return ()
  in
  let on_witness who i = Lwt.return () in
  let get_value i = Lwt.fail (Failure "no_value") in
  let inject_buffer = Lwt_buffer.create () in
  let election_timeout_buffer = Lwt_buffer.create() in
  let inject_event e = Lwt_buffer.add e inject_buffer in

  Mem_store.make_mem_store "MEM#store" >>= fun store ->
  Mem_tlogcollection.make_mem_tlog_collection "MEM#tlog" >>= fun tlog_coll ->
  let constants = {me = me; others = others;
		   send = send;
		   receive = receive;
		 
		   get_value = get_value;
		   on_accept = on_accept;
		   on_consensus = on_consensus;
		   on_witness = on_witness;
		   quorum_function = Multi_paxos.quorum_function;
		   forced_master = None;
		   store = store;
		   tlog_coll = tlog_coll;
		   other_cfgs = [];
		   lease_expiration = 60;
		   inject_event = inject_event;
		  } in
  let continue = ref 2 in
  let c0_t () =
    let expected prev_key key =
      log ~me "c0 from %s to %s" (Multi_paxos_type.show_transition prev_key) 
	(Multi_paxos_type.show_transition key) >>= fun () ->
      match key with
	| (Multi_paxos_type.Stable_master x) ->
	  if !continue = 0 then Lwt.return (Some x) else
	    let () = continue := (!continue -1) in Lwt.return None
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
    >>= fun result -> log "after loop"
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
    log ~me:source "%s --- %s ----->? %s" source (string_of msg) target >>= fun () ->
    let q = get_q target in
    Lwt_buffer.add (Mp_msg.MPMessage.generic_of msg,source) q >>= fun () ->
    log ~me:source "added to buffer of %s" target
  in
  let receive target = Lwt.fail (Failure "don't call receive") in
  let peek target    = Lwt.fail (Failure "don't call peek") in
  let get_buffer = get_q in
  let run () = Lwt_unix.sleep 2.0 in
  let register (xs:(string * (string * int)) list) = () in
  get_buffer, send, receive, peek, run, register

let build_simulation events =
  let state =
    {scenario = events;
     waiters = Hashtbl.create 7
    } in
  let rec maybe_wakeup_someone () =
    if Hashtbl.length state.waiters = 0
    then Lwt.return ()
    else
      begin
	match state.scenario with
	  | [] -> Lwt.return ()
	  | (m,s,t) :: rest ->
	    if not (Hashtbl.mem state.waiters t) then
	      Lwt.return ()
	    else
	      let awakener = Hashtbl.find state.waiters t in
	      let () = Hashtbl.remove state.waiters t in
	      let () = state.scenario <- rest in
	      Lwt.wakeup awakener (m,s);
	      maybe_wakeup_someone()

      end
  in
  let receive target =
    let give (m,s) =
      log ~me:target "receiving %s from %s" (string_of m) s >>= fun () ->
      Lwt.return (m,s)
    in
    match state.scenario with
      | [] -> failwith "end of scenario"
      | (m,s,t) :: rest ->

	if t = target then
	  let () = state.scenario <- rest in
	  maybe_wakeup_someone() >>= fun () -> give (m,s)
	else
	  begin
	    let waiter, awakener = Lwt.wait () in
	    let () = Hashtbl.add state.waiters target awakener in
	    waiter >>= fun (m,s) -> give (m,s)
	  end
  in
  let peek target = Lwt.fail (Failure "don't peek in tests")
  in

  let send msg source target =
    log ~me:source "sends %s to %s" (string_of msg)  target >>= fun () ->
    Lwt.return () in
  send, receive, peek

let build_tcp () =
  let (m : messaging) = new tcp_messaging ("127.0.0.1", 7777) in
  let network = network_of_messaging m in
  network




let test_simulation scenario () =
  let network = build_simulation scenario in
  let send, receive, peek = network in
  (* TODO *)
  let get_buffer who = Lwt_buffer.create () in
  let me = "c0" in
  let current_n = 42L in
  let current_i = 666L in
  let on_accept me (v,n,i) =
    log ~me "on_accept: (%s,%s)" (sn2s n) (sn2s i)
  in
  let on_consensus me (v,n,i) =
    log ~me "on_consensus: (%s,%s) " (sn2s n) (sn2s i)
    >>= fun () ->
    Lwt.return (Store.Ok None)
  in
  let on_witness who i = Lwt.return () in
  let get_value i = Lwt.fail (Failure "no value") in
  let inject_buffer = Lwt_buffer.create () in
  let election_timeout_buffer = Lwt_buffer.create () in
  let client_buffer = Lwt_buffer.create () in
  let inject_event e = Lwt_buffer.add e inject_buffer in
  Mem_store.make_mem_store "MEM#store" >>= fun store ->
  Mem_tlogcollection.make_mem_tlog_collection "MEM#tlog" >>= fun tlog_coll ->
  let constants = {me = me;
		   others = ["c1";"c2"];
		   send = send;
		   receive = receive;
		 
		   get_value = get_value;
		   on_accept = on_accept me;
		   on_consensus = on_consensus me;
		   on_witness = on_witness;
		   quorum_function = Multi_paxos.quorum_function;
		   forced_master = None;
		   store = store;
		   tlog_coll = tlog_coll;
		   other_cfgs = [];
		   lease_expiration = 60;
		   inject_event = inject_event;
		  } in
  let c0_t () =
    let expected prev_key key =
      log ~me "c0 from %s to %s" (Multi_paxos_type.show_transition prev_key) 
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
    >>= fun (v', n, i) ->
    log ~me "consensus reached: (%s,%s)" (sn2s n) (sn2s i)
  in
  let cx_t me other =
    let inject_buffer = Lwt_buffer.create () in
    let election_timeout_buffer = Lwt_buffer.create () in
    let client_buffer = Lwt_buffer.create () in
    let constants =
      {constants with me=me;
	others = ["c0";other];
	on_accept = on_accept me;
	on_consensus = on_consensus me;
	inject_event = (fun e -> Lwt_buffer.add e inject_buffer);
      }
    in
    let expected prev_key key =
      log ~me "node from %s to %s" 
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
    Multi_paxos_fsm.expect_run_forced_slave constants buffers expected 50 current_i
    >>= fun result ->
    log ~me "node done." >>= fun () ->
    Lwt.return ()
  in
  Lwt.pick [c0_t (); cx_t "c1" "c2"; cx_t "c2" "c1"; ] >>= fun () ->
  log "after pick"


let ideal =
  let v = Value.create "c0" in
  [
  (Prepare(42L,0L)         , "c0", "c1");
  (Prepare(42L,0L)         , "c0", "c2");
  (Promise(42L, 0L, None), "c1", "c0");
  (Promise(42L, 0L, None), "c2", "c0");
  (Accept(42L, 0L, v)     , "c0", "c1");
  (Accept(42L, 0L, v)     , "c0", "c2");
  (Accepted(42L, 0L),      "c1", "c0");
  (Accepted(42L, 0L),      "c2", "c0");
  ];;

let c2_fails =
  let v = Value.create "c0" in
  [
    (Prepare (42L,0L)          , "c0", "c1");
    (Promise(42L, 666L, None), "c1", "c0");
  (* (Promise(42, 0, None), "c2", "c0"); *)
    (Accept (42L, 666L, v)   , "c0", "c1");
    (Accept (42L, 666L, v)   , "c0", "c2");
    (Accepted(42L, 666L)     , "c1", "c0");
(* (Accepted(42, 0)     , "c2", "c0"); *)
];;

let c2_promised =
  let v = Value.create "c0" in
  let v2 = Value.create "other" in
  [
    (Prepare (42L,0L)             , "c0", "c1");
    (Prepare (42L,0L)             , "c0", "c2");
    (Promise(42L, 0L, None)   , "c1", "c0");
    (Promise(42L, 0L, Some v2) , "c2", "c0");
    (Accept(42L,0L,v)         , "c0", "c1");
    (Accept(42L,0L,v)         , "c0", "c1");
    (Accepted(42L, 0L)        , "c1", "c0");
    (Accepted(42L, 0L)        , "c2", "c0");
];;

let c1_nak =
  (* let v = Value.create "master"  "c0" in *)
  [
    (* (Prepare 42             , "c0", "c1");
       (Prepare 42             , "c0", "c2"); *)
    (Nak (42L,(43L,0L))             , "c1", "c0");
    (Promise (42L, 0L, None)  , "c2", "c0");
    (Promise (43L, 0L, None)  , "c1", "c0");
    (* (Accept(43,0, v)         , "c0", "c1");
       (Accept(43,0, v)         , "c0", "c1"); *)
    (Accepted(43L, 0L),         "c1", "c0");
    (Accepted(43L, 0L),         "c2", "c0");
];;

let xtodo () =
  OUnit.todo "re-enable"

(* Lwt_main.run (test ideal);; *)
open OUnit
let w = lwt_test_wrap
let suite = "basic" >::: [
  "singleton_perfect" >:: w (test_generic build_perfect 1);
  "pair_perfect"  >:: w (test_generic build_perfect 2);
  "trio"          >:: w (test_generic build_perfect 3);
  "quartet"       >:: w (test_generic build_perfect 4);
  "quintet"       >:: w (test_generic build_perfect 5);
  "sextet"        >:: w (test_generic build_perfect 6);
  (*
  "c2_fails"      >:: w (test_simulation c2_fails);
  "c1_nak"        >:: w (test_simulation c1_nak);
  *)
  "c2_fails"      >:: xtodo;
  "c1_nak"        >:: xtodo;
  "pair_tcp"      >:: xtodo;
  (* "pair_tcp"      >:: w (test_generic build_tcp 2); *)
  "master_loop_1" >:: w (test_master_loop build_perfect);
];;



