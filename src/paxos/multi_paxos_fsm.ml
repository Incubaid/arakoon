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

open Lwt
open Messaging
open Mp_msg
open MPMessage

open Update
open Lwt_buffer
open Fsm
open Multi_paxos_type
open Multi_paxos


(* a forced master always suggests himself *)
let forced_master_suggest constants (n,i) () =
  let me = constants.me
  and others = constants.others in
  let n' = update_n constants n in
  mcast constants (Prepare n') >>= fun () ->
  log ~me "forced_master_suggest: suggesting n=%s" (Sn.string_of n') >>= fun () ->
  let needed = constants.quorum_function (List.length others + 1) in
  let v = Update.make_update_value (Update.make_master_set me) in
  let ballot = (needed -1, [me] ) in
  let v_lims = [v] in
  let i_lim = Some (me,i) in
  let state = (n', i, ballot, v, v_lims, i_lim) in
  Lwt.return (Promises_check_done state)

(* in case of election, everybody suggests himself *)
let election_suggest constants state () =
  let me = constants.me in
  let (n,i) = state in
  let n' = update_n constants n in
  log ~me "election_suggest: Master undecided, starting election n=%s" (Sn.string_of n') >>= fun () ->
  let v = Update.make_update_value (Update.make_master_set me) in
  let others = constants.others in
  start_election_timeout constants n' >>= fun () ->
  mcast constants (Prepare n') >>= fun () ->
  let needed = constants.quorum_function (List.length others + 1) in
  let ballot = (needed -1, [me] ) in
  let v_lims = [v] in
  let i_lim = Some (me,i) in
  let state = (n', i, ballot, v, v_lims, i_lim) in
  Lwt.return (Promises_check_done state)



(* a pending slave that is waiting for a prepare or a nak
   in order to discover a master *)
let slave_waiting_for_prepare constants current_i event =
  match event with 
    | FromNode(msg,source) ->
      begin
	let send = constants.send in
	let me = constants.me in
	log ~me "slave_waiting_for_prepare: %S" (MPMessage.string_of msg) >>= fun () ->
	match msg with
	  | Prepare(n) when n >= 0L ->
	    begin
	      let reply = Promise(n,current_i,None) in
	      log ~me "replying with %S" (string_of reply) >>= fun () ->
	      send reply me source >>= fun () ->
	      Lwt.return (Slave_wait_for_accept (n, current_i, None))
	    end
	  | Prepare(n) when n < 0L ->
	    begin
	      log ~me "forced_slave ignoring Prepare with n<0" >>= fun () ->
	      Lwt.return (Slave_waiting_for_prepare current_i)
	    end
	  | Nak(n',(n2, i2)) when n' = -1L ->
	    begin
	      log ~me "fake prepare response: discovered master" >>= fun () ->
	      Lwt.return (Slave_discovered_other_master (source, current_i, n2, i2))
	    end
	  | Nak(n',(n2, i2)) when i2 > current_i ->
	    begin
	      log ~me "got %s => go to catchup" (string_of msg) >>= fun () ->
	      Lwt.return (Slave_discovered_other_master (source, current_i, n2, i2))
	    end
	  | Nak(n',(n2, i2)) when i2 = current_i ->
	    begin
	      log ~me "got %s => we're in sync" (string_of msg) >>= fun () ->
	(* pick in @ steady state *)
	      constants.get_value (Sn.pred i2) >>= fun p ->
	      match p with
		| None ->
		  begin
		    let () = assert (i2 = Sn.start) in
		    Lwt.return (Slave_waiting_for_prepare Sn.start)
		  end
		| Some v ->
		  begin
		    let vos = Log_extra.option_to_string Value.string_of p in
		    log ~me "reentering steady state @(%s,%s,%S)" (Sn.string_of n2) (Sn.string_of i2) vos
		    >>= fun () ->
		    Lwt.return (Slave_steady_state (n2, i2, v))
		  end
	    end
	  | _ -> log ~me "dropping unexpected %s" (string_of msg) >>= fun () ->
	    Lwt.return (Slave_waiting_for_prepare current_i)
      end
    | _ -> paxos_fatal constants.me "Slave_waiting_for_prepare only wants FromNode"


(* a potential master is waiting for promises and if enough
   promises are received the value is accepted and Accept is broadcasted *)
let promises_check_done constants state () =
  let n, i, ballot, wanted, v_lims, i_lim = state in
  let (to_receive, _) = ballot in
  let me = constants.me in
  let others = constants.others in
  log ~me "promises_check_done:to_receive=%i" to_receive >>= fun () ->
  if to_receive = 0 then
    begin
      choose_value me wanted v_lims >>= function
	| None ->
	  begin
	    log ~me "promises_check_done: choose_value conflict: back to suggest" >>= fun () ->
	    if am_forced_master constants me
	    then
	      Lwt.return (Forced_master_suggest (n,i))
	    else if is_election constants then
	      Lwt.return (Election_suggest (n,i))
	    else
	      paxos_fatal me "slave checking for promises"
	  end
	| Some v' ->
	  begin
	    (match i_lim with
	      | None -> Lwt.return (n,Sn.start,v',None)
	      | Some (source',i') ->
		Lwt.return (n, i',v',Some source'))
	    >>= fun (n,future_i,v,source) ->
	    begin
	      if future_i > i then
		let other_cfgs = constants.other_cfgs
		and store = constants.store
		and tlog_coll = constants.tlog_coll in
		let source' = match source with 
		  | Some s -> s 
		  | None -> failwith "None should not happen here"
		in
		log ~me "promises_check_done: first catchup till %S from %s" (Sn.string_of future_i) source' >>= fun () ->
		Catchup.catchup me other_cfgs (store, tlog_coll) i source' (n, future_i)
		 >>= fun (future_n', current_i', vo') ->
		Lwt.return (future_n',current_i')
	      else Lwt.return (n, future_i)
	    end >>= fun (n',new_i) ->
	    if n' > n then
	      begin
		log ~me "promises_check_done: back to election" >>= fun () ->
		if am_forced_master constants me
		then Lwt.return (Forced_master_suggest (n',new_i))
		else
		  if is_election constants
		  then Lwt.return (Election_suggest (n',new_i))
		  else paxos_fatal me "slave checking for promises"
	      end
	    else
	      begin
		log ~me "promises_check_done: accepting value %s and mcasting" (Value.string_of v') >>= fun () ->
		constants.on_accept (v,n,new_i) >>= fun () ->
		let msg = Accept(n,new_i,v) in
		mcast constants msg >>= fun () ->
		let nnodes = List.length others + 1 in
		let needed = constants.quorum_function nnodes in
		let new_ballot = (needed-1 , [me] ) in
		Lwt.return (Accepteds_check_done (None, n, new_i, new_ballot, v))
	      end
	  end
    end
  else
    Lwt.return (Wait_for_promises state)

(* a potential master is waiting for promises and receives a msg *)
let wait_for_promises constants state event =
  let me = constants.me in
  match event with
    | FromNode (msg,source) ->
      begin
	let (n, i, ballot, wanted, v_lims, i_lim) = state in
	let to_receive, already_voted = ballot in
	let drop msg reason =
	  log ~me "dropping %s because: %s" (string_of msg) reason >>= fun () ->
	  Lwt.return (Wait_for_promises state)
	in
	log ~me "wait_for_promises:i=%S to_receive=%i" (Sn.string_of i) to_receive >>= fun () ->
	begin
	  log ~me "wait_for_promises:: received %S" (string_of msg) >>= fun () ->
	  match msg with
	    | Promise (n' ,i, limit) when n' < n ->
	      let reason = Printf.sprintf "old promise (%s < %s)" 
		(Sn.string_of n') (Sn.string_of n) in
	      drop msg reason
	    | Promise (n' ,new_i, limit) when n' = n ->
	      if List.mem source already_voted then
		drop msg "duplicate promise"
	      else
		let v_lims' = maybe_add v_lims limit in
		let to_receive = to_receive - 1 in
		let ballot' = (to_receive, source :: already_voted) in
		let new_ilim = match i_lim with
		  | Some (source',i') -> if i' < new_i then Some (source,new_i) else i_lim
		  | None -> Some (source,new_i)
		in
		let state' = (n, i, ballot', wanted, v_lims', new_ilim) in
		Lwt.return (Promises_check_done state')
	    | Promise (n' ,i, limit) when n' > n ->
	      begin
		paxos_fatal me "wait_for_promises:: received %S with n'=%s > my n=%s FATAL" 
		  (string_of msg) (Sn.string_of n') (Sn.string_of n)
	      end
	    | Nak (n',(n'',i')) when n' < n ->
	      begin
		log ~me "wait_for_promises:: received old %S (ignoring)" (string_of msg) 
		>>= fun () ->
		Lwt.return (Wait_for_promises state)
	      end
	    | Nak (n',(n'',i')) when n' > n ->
	      paxos_fatal me "wait_for_promises:: received %S with n'=%s > my n=%s FATAL"
		(string_of msg) (Sn.string_of n') (Sn.string_of n)
	    | Nak (n',(n'',i')) when n' = n ->
	      begin
		log ~me "wait_for_promises:: received %S for my Prep" (string_of msg) 
		>>= fun () ->
		if am_forced_master constants me
		then
		  begin
		    log ~me "wait_for_promises; forcing new master suggest" >>= fun () ->
		    let n''' = max n n'' in
		    Lwt.return (Forced_master_suggest (n''',i))
		  end
		else if is_election constants
		then
		  begin
		    log ~me "wait_for_promises; discovered other node" 
		    >>= fun () ->
		    if n'' > n then
		      Lwt.return (Slave_discovered_other_master (source,i,n'',i'))
		    else
		      let n''' = max n n'' in
		      Lwt.return (Election_suggest (n''',i))
		  end
		else (* forced_slave *) (* this state is impossible?! *)
		  begin
		    log ~me "wait_for_promises; forced slave back waiting for prepare" >>= fun () ->
		    Lwt.return (Slave_waiting_for_prepare i)
		  end
	      end
	    | Prepare n' when n' < n ->
	      begin
		let i = match i_lim with
		  | None -> 0L
		  | Some (_source,i) -> i
		in
		let reply = Nak (n', (n,i))  in
		log ~me "wait_for_promises:: Nak-ing lower prepare" >>= fun () ->
		constants.send reply me source >>= fun () ->
		constants.send (Prepare n) me source >>= fun () ->
		log ~me "wait_for_promises:: prepare re-sent mode, keep waiting" >>= fun () ->
		Lwt.return (Wait_for_promises state)
	      end
	    | Prepare n' when n' > n ->
	      begin
		let i = match i_lim with
		  | None -> 0L
		  | Some (_source,i) -> i
		in
		if (is_election constants) || not (am_forced_master constants me)
		then
		  let reply = Promise(n',i,None) in
		  log ~me "replying with %S" (string_of reply) >>= fun () ->
		  constants.send reply me source >>= fun () ->
		  Lwt.return (Slave_wait_for_accept (n', i, None))
		else
		  let reply = Nak (n', (n,i))  in
		  constants.send reply me source >>= fun () ->
		  Lwt.return (Wait_for_promises state)
	      end
	    | Prepare n' when n' = n ->
	      begin
		if am_forced_master constants me
		then
		  begin
		    log ~me "wait_for_promises:dueling; forcing new master suggest" >>= fun () ->
		    let reply = Nak (n', (n,i))  in
		    constants.send reply me source >>= fun () ->
		    Lwt.return (Forced_master_suggest (n',i))
		  end
		else if is_election constants
		then
		  begin
		    log ~me "wait_for_promises:dueling; forcing new election suggest" >>= fun () ->
		    let reply = Nak (n', (n,i))  in
		    constants.send reply me source >>= fun () ->
		    Lwt.return (Election_suggest (n,i))
		  end
		else (* forced slave *)
		  begin
		    let reply = Nak(n',(n,i)) in
		    log ~me "wait_for_promises: forced_slave replying with %S" (string_of reply) >>= fun () ->
		    constants.send reply me source >>= fun () ->
		    Lwt.return (Wait_for_promises state)
		  end
	      end
	    | Accept (n',_i,_v) when n' < n ->
	      begin
		log ~me "wait_for_promises: ignoring old Accept %s" (Sn.string_of n') 
		>>= fun () ->
		Lwt.return (Wait_for_promises state)
	      end
	    | Accept (n',_i,_v) when n' >= n ->
	      begin
		log ~me "wait_for_promises: received %S -> back to fake prepare" 
		  (string_of msg) >>= fun () ->
		Lwt.return (Slave_fake_prepare i)
	      end
	    | Accepted (n',_i) when n' < n ->
	      begin
		log ~me "wait_for_promises: ignoring old Accepted %s" (Sn.string_of n') 
		>>= fun () ->
		Lwt.return (Wait_for_promises state)
	      end
	    | Accepted (n',_i) when n' >= n ->
	      paxos_fatal me "wait_for_promises:: received %S with n'=%s > my n=%s FATAL" 
		(string_of msg) (Sn.string_of n') (Sn.string_of n)
	end
      end
    | ElectionTimeout n' ->
      let (n,i,_,_,_,_) = state in
      if n' = n then
	begin
	  log ~me "wait_for_promises: election timeout, restart from scratch"	  
	  >>= fun () ->
	  Lwt.return (Election_suggest (n,i))
	end
      else
	begin
	  Lwt.return (Wait_for_promises state)
	end
    | LeaseExpired _ ->
      paxos_fatal me "wait_for_promises: don't want LeaseExpired"
    | FromClient _ -> 
      paxos_fatal me "wait_for_promises: don't want FromClient"

(* a (potential or full) master is waiting for accepteds and if he has received
   enough, concensus is reached and he becomes a full master *)
let accepteds_check_done constants state () =
  let (mo,n,i,ballot,v) = state in
  let me = constants.me in
  let needed, already_voted = ballot in
  if needed = 0 then
    begin
      log ~me "accepted_check_done :: we're done! returning %S %s %s"
	(Value.string_of v) (Sn.string_of n) ( Sn.string_of i )
      >>= fun () ->
      Lwt.return (Master_consensus (mo,v,n,i))
    end
  else
    Lwt.return (Wait_for_accepteds (mo,n,i,ballot,v))
      
(* a (potential or full) master is waiting for accepteds and receives a msg *)
let wait_for_accepteds constants state (event:paxos_event) =
  let me = constants.me in
  match event with
    | FromNode(msg,source) ->
      begin
    (* TODO: what happens with the client request
       when I fall back to a state without mo ? *)
	let (mo,n,i,ballot,v) = state in
	let drop msg reason =
	  log ~me "dropping %s because : '%s'" (MPMessage.string_of msg) reason
	  >>= fun () ->
	  Lwt.return (Wait_for_accepteds state)
	in
	let needed, already_voted = ballot in
	log ~me "wait_for_accepteds(%i to go) got %S from %s" needed 
	  (MPMessage.string_of msg) source >>= fun () ->
	match msg with
	  | Accepted (n',i') when (n',i')=(n,i)  ->
	    begin
	      if List.mem source already_voted then
		let reason = Printf.sprintf "%s already voted" source in
		drop msg reason
	      else
		let ballot' = needed -1, source :: already_voted in
		Lwt.return (Accepteds_check_done (mo,n,i,ballot',v))
	    end
	  | Accepted (n',i') when n' = n && i' < i ->
	    begin
	      log ~me "wait_for_accepteds: received older Accepted for my n: ignoring" >>= fun () ->
	      Lwt.return (Wait_for_accepteds state)
	    end
	  | Accepted (n',i') when n' > n ->
	    paxos_fatal me "wait_for_accepteds:: received %S with n'=%s > my n=%s FATAL" (string_of msg) (Sn.string_of n') (Sn.string_of n)
	  | Accepted (n',i') when n' < n ->
	    begin
	      let reason = Printf.sprintf "dropping old %S we're @ (%s,%s)" (string_of msg) (Sn.string_of n) (Sn.string_of i) in
	      drop msg reason
	    end
	  | Promise(n',i', limit) ->
	    if n' <= n then
	      begin
		let reason = Printf.sprintf
		  "already reached consensus on (%s,%s)" 
		  (Sn.string_of n) (Sn.string_of i) 
		in
		drop msg reason
	      end
	    else
	      paxos_fatal me "future Promise(%s,%s), local (%s,%s)"
		(Sn.string_of n') (Sn.string_of i') (Sn.string_of n) (Sn.string_of i)
	  | Prepare n' when n' < n ->
	    begin
	      log ~me "wait_for_accepteds: received older Prepare" >>= fun () ->
	      let reply0 = Nak (n', (n,i)) in
	      constants.send reply0 me source >>= fun () ->
	      let reply1 = Accept(n,i,v) in
	      constants.send reply1 me source >>= fun () ->
	      log ~me "replied %S; follow_up:%S" (MPMessage.string_of reply0) 
		(MPMessage.string_of reply1) >>= fun () ->
	      Lwt.return (Wait_for_accepteds state)
	    end
	  | Prepare n' when n' >= n ->
	    begin
	      if am_forced_master constants me
	      then
		Lwt.return (Forced_master_suggest (n',i))
	      else if is_election constants
	      then
		begin
		  match mo with
		    | None -> Lwt.return ()
		    | Some finished -> 
		      let msg = "lost master role during wait_for_accepteds while handling client request" in
		      let rc = Arakoon_exc.E_NOT_MASTER in
		      let result = Store.Update_fail (rc, msg) in
		      finished result
		end >>= fun () ->
	      let reply = Promise(n',i,None) in
	      log ~me "wait_for_accepteds: replying with %S to %s" (MPMessage.string_of reply) source >>= fun () ->
	      constants.send reply me source >>= fun () ->
	      Lwt.return (Slave_wait_for_accept (n',i, None))
	      else
		paxos_fatal me "wait_for_accepteds: received %S when forced slave, forced slave should never get in wait_for_accepteds in the first place!" (string_of msg)
	    end
	  | Nak (n',i) ->
	    begin
	      log ~me "wait_for_accepted: ignoring %S from %s when collecting accepteds" (MPMessage.string_of msg) source >>= fun () ->
	      Lwt.return (Wait_for_accepteds state)
	    end
	  | Accept (n',_i,_v) when n' < n ->
	    begin
	      log ~me "wait_for_accepted: dropping old Accept %S" (string_of msg) >>= fun () ->
	      Lwt.return (Wait_for_accepteds state)
	    end
	  | Accept (n',i',v') when (n',i',v')=(n,i,v) ->
	    begin
	      log ~me "wait_for_accepted: ignoring extra Accept %S" (string_of msg) >>= fun () ->
	      Lwt.return (Wait_for_accepteds state)
	    end
	  | Accept (n',i',v') when n' >= n ->
	    paxos_fatal me "wait_for_accepteds: received %S from future: FATAL" 
	      (string_of msg)
      end
    | FromClient (vo, cb) -> paxos_fatal me "no FromClient should get here"
    | LeaseExpired n' -> paxos_fatal me "no LeaseExpired should get here"
    | ElectionTimeout n' -> 
      begin
	let (_,n,i,ballot,v) = state in
	log ~me "wait_for_accepteds : election timeout " >>= fun () ->
	if n' < n then
	  begin
	    log ~me "ignoring old timeout %s<%s" (Sn.string_of n') (Sn.string_of n) 
	    >>= fun () ->
	  Lwt.return (Wait_for_accepteds state)
	  end
	else if n' = n then
	  begin
	    if (am_forced_master constants me) then
	      begin
		log ~me "going to RESEND Accept messages" >>= fun () ->
		let needed, already_voted = ballot in
		let msg = Accept(n,i,v) in
		let silent_others = List.filter (fun o -> not (List.mem o already_voted)) 
		  constants.others in
		Lwt_list.iter_s (fun o -> constants.send msg me o) silent_others >>= fun () ->
		mcast constants msg >>= fun () ->
		Lwt.return (Wait_for_accepteds state)
	      end
	    else
	      begin
		log ~me "TODO: election part of election timeout" >>= fun () ->
		Lwt.return (Wait_for_accepteds state)
	      end
		
	  end
	else
	  begin
	    Lwt.return (Wait_for_accepteds state)
	  end
      end
	  




(* the state machine translator *)

type product_wanted =
  | Nop
  | Node_only
  | Full
  | Node_and_inject
  | Node_and_timeout
  | Node_and_inject_and_timeout

let machine constants = 
  let nop = Nop in
  let node_only = Node_only in
  let full = Full in
  let node_and_timeout = Node_and_timeout in
  function
  | Forced_master_suggest state ->
    (Unit_arg (forced_master_suggest constants state), nop)

  | Slave_fake_prepare i ->
    (Unit_arg (Slave.slave_fake_prepare constants i), nop)
  | Slave_waiting_for_prepare i ->
    (Msg_arg (slave_waiting_for_prepare constants i), node_only)
  | Slave_wait_for_accept state ->
    (Msg_arg (Slave.slave_wait_for_accept constants state), node_only)
  | Slave_steady_state state ->
    (Msg_arg (Slave.slave_steady_state constants state), full)
  | Slave_discovered_other_master state ->
    (Unit_arg (Slave.slave_discovered_other_master constants state), nop)

  | Wait_for_promises state ->
    (Msg_arg (wait_for_promises constants state), node_and_timeout)
  | Promises_check_done state ->
    (Unit_arg (promises_check_done constants state), nop)
  | Wait_for_accepteds state ->
    (Msg_arg (wait_for_accepteds constants state), node_and_timeout)
  | Accepteds_check_done state ->
    (Unit_arg (accepteds_check_done constants state), nop)

  | Master_consensus state ->
    (Unit_arg (Master.master_consensus constants state), nop)
  | Stable_master state ->
    (Msg_arg (Master.stable_master constants state), full)
  | Master_dictate state ->
    (Unit_arg (Master.master_dictate constants state), nop)

  | Election_suggest state ->
    (Unit_arg (election_suggest constants state), nop)

let trace_transition me key =
      log ~me "new transition: %s" (show_transition key)

type ready_result =
  | Inject_ready
  | Client_ready
  | Node_ready
  | Election_timeout_ready

let prio = function
  | Inject_ready -> 0
  | Election_timeout_ready -> 1
  | Node_ready   -> 2
  | Client_ready -> 3


type ('a,'b,'c) buffers = 
    {client_buffer : 'a Lwt_buffer.t; 
     node_buffer   : 'b Lwt_buffer.t;
     inject_buffer : 'c Lwt_buffer.t;
     election_timeout_buffer: 'c Lwt_buffer.t;
    } 
let make_buffers (a,b,c,d) = {client_buffer = a;
			      node_buffer = b;
			      inject_buffer = c;
			      election_timeout_buffer = d;}
      
let rec paxos_produce buffers
    constants product_wanted =
  let me = constants.me in
  let ready_from_inject () =
    Lwt_buffer.wait_for_item buffers.inject_buffer >>= fun () ->
    Lwt.return Inject_ready
  in
  let ready_from_client () =
    Lwt_buffer.wait_for_item buffers.client_buffer >>= fun () ->
    Lwt.return Client_ready
  in
  let ready_from_node () =
    Lwt_buffer.wait_for_item buffers.node_buffer >>= fun () ->
    Lwt.return Node_ready
  in
  let ready_from_election_timeout () =
    Lwt_buffer.wait_for_item buffers.election_timeout_buffer >>= fun () ->
    Lwt.return Election_timeout_ready
  in
  let waiters =
    match product_wanted with
      | Node_only ->
	[ready_from_node ();]
      | Full ->
	[ready_from_inject();ready_from_node ();ready_from_client ();]
      | Node_and_inject ->
	[ready_from_inject();ready_from_node ();]
      | Node_and_timeout ->
	[ready_from_election_timeout (); ready_from_node();]
      | Node_and_inject_and_timeout ->
	[ready_from_inject();ready_from_election_timeout () ;ready_from_node()]
  in
  Lwt.catch 
    (fun () ->
      Lwt.pick waiters >>= function 
	| Inject_ready ->
	  begin
	    log ~me "taking from inject" >>= fun () ->
	    Lwt_buffer.take buffers.inject_buffer
	  end
	| Client_ready ->
	  begin
	    log ~me "taking from client" >>= fun () ->
	    Lwt_buffer.take buffers.client_buffer >>= fun x ->
	    Lwt.return (FromClient x)
	  end
	| Node_ready ->
	  begin
	    log ~me "taking from node" >>= fun () ->
	    Lwt_buffer.take buffers.node_buffer >>= fun (msg,source) ->
	    let msg2 = MPMessage.of_generic msg in
	    Lwt.return (FromNode (msg2,source))
	  end
	| Election_timeout_ready ->
	  begin
	    log ~me "taking from timeout" >>= fun () ->
	    Lwt_buffer.take buffers.election_timeout_buffer 
	  end
    ) 
    (fun e -> log ~me "ZYX %s" (Printexc.to_string e) >>= fun () -> Lwt.fail e)

(* the entry methods *)

let run_forced_slave constants buffers new_i =
  let me = constants.me in
  log ~me "+starting FSM for forced_slave." >>= fun () ->
  let trace = trace_transition me in
  let produce = paxos_produce buffers constants in
  Lwt.catch 
    (fun () ->
      Fsm.loop ~trace produce 
	(machine constants) (Slave.slave_fake_prepare constants new_i)
    ) 
    (fun e ->
      log ~me "FSM BAILED due to uncaught exception %s" (Printexc.to_string e) 
      >>= fun () -> Lwt.fail e
    )

let run_forced_master constants buffers current_i =
  let me = constants.me in
  log ~me "+starting FSM for forced_master." >>= fun () ->
  let current_n = 0L in
  let trace = trace_transition me in
  let produce = paxos_produce buffers constants in
  Lwt.catch 
    (fun () ->
      Fsm.loop ~trace produce 
	(machine constants) 
	(forced_master_suggest constants (current_n,current_i))
    ) 
    (fun e ->
      log ~me "FSM BAILED due to uncaught exception %s" (Printexc.to_string e)
      >>= fun () -> Lwt.fail e
    )

let run_election constants buffers current_i =
  let me = constants.me in
  log ~me "+starting FSM election." >>= fun () ->
  let current_n = Sn.start in
  let trace = trace_transition me in
  let produce = paxos_produce buffers constants in
  Lwt.catch 
    (fun () ->
      Fsm.loop ~trace produce 
	(machine constants) (election_suggest constants (current_n,current_i))
    ) 
    (fun e ->
      log ~me "FSM BAILED due to uncaught exception %s" (Printexc.to_string e) 
      >>= fun () -> Lwt.fail e
    )

let expect_run_forced_slave constants buffers expected step_count new_i =
  let me = constants.me in
  log ~me "+starting forced_slave FSM with expect" >>= fun () ->
  let produce = paxos_produce buffers constants in
  Lwt.catch 
    (fun () ->
      Fsm.expect_loop expected step_count Start_transition produce 
	(machine constants) (Slave.slave_fake_prepare constants new_i)
    ) 
    (fun e ->
      log ~me "FSM BAILED due to uncaught exception %s" (Printexc.to_string e) 
      >>= fun () -> Lwt.fail e
    )

let expect_run_forced_master constants buffers expected step_count current_n current_i =
  let me = constants.me in
  let produce = paxos_produce buffers constants in
  log ~me "+starting forced_master FSM with expect" >>= fun () ->
  Lwt.catch 
    (fun () ->
      Fsm.expect_loop expected step_count Start_transition produce 
	(machine constants) 
	(forced_master_suggest constants (current_n,current_i))
    ) 
    (fun e ->
      log ~me "FSM BAILED due to uncaught exception %s" (Printexc.to_string e)
      >>= fun () -> Lwt.fail e
    )
