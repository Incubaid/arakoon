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

open Multi_paxos_type
open Multi_paxos
open Lwt
open Mp_msg.MPMessage
open Update

(* a forced slave or someone who is outbidded sends a fake prepare
   in order to receive detailed info from the others in a Nak msg *)
let slave_fake_prepare constants current_i () =
  (* fake a prepare, and hopefully wake up a dormant master *)
  let me = constants.me in
  log ~me "slave_fake_prepare: sending Prepare(-1)" >>= fun () ->
  let fake = Prepare( Sn.of_int (-1), current_i) in
  Multi_paxos.mcast constants fake >>= fun () ->
  Lwt.return (Slave_waiting_for_prepare current_i)

(* a pending slave that is in sync on i and is ready
   to receive accepts *)
let slave_steady_state constants state event =
  let (n,i,previous, from_here) = state in
  let me = constants.me in
  match event with
    | FromNode (msg,source) ->
      begin
	let send = constants.send in
	log ~me "slave_steady_state n:%s i:%s got %S from %s" 
	  (Sn.string_of n) (Sn.string_of i) (string_of msg) source 
	>>= fun () ->
	match msg with
	  | Accept (n',i',v) when (n',i') = (n,i) ->
	    begin
	      let reply = Accepted(n,i) in
	      begin
		if from_here then
		  constants.on_consensus (previous, n,Sn.pred i) 
		else
		  begin (* don't call on_consensus, 
			   but still enter the master_update in the store;
			   ... THIS IS BAD ...
			*)
		    let Value.V(us ) = previous in
		    let update,_ = Update.from_buffer us 0 in
		    begin
		      match update with
			| Update.MasterSet(m,l) -> 
			  constants.store # set_master_no_inc m l 
			| _ -> Lwt.return ()
		    end >>= fun () ->
		    Lwt_log.debug_f "SKIPPING %S (paxos -> multipaxos)" 
		      (Value.string_of previous) >>= fun () ->
		    Lwt.return (Store.Ok None) 
		  end
	      end >>= fun _ ->
	      constants.on_accept(v,n,i) >>= fun () ->
	      log ~me "steady_state :: replying with %S" (string_of reply) 
	      >>= fun () ->
	      send reply me source >>= fun () ->
	      Lwt.return (Slave_steady_state (n, Sn.succ i, v, true))
	    end
	  | Accept (n',i',v) when n'=n && i'<i ->
	    begin
	      log ~me "slave_steady_state received old %S for my n, ignoring" 
		(string_of msg) >>= fun () ->
	      Lwt.return (Slave_steady_state state)
	    end
	  | Accept (n',i',v) ->
	    begin
	      log ~me "slave_steady_state foreign (%s,%s) from %s <> local (%s,%s) back to fake prepare"
		(Sn.string_of n') (Sn.string_of i') source (Sn.string_of  n) (Sn.string_of  i)
	      >>= fun () ->
	      Lwt.return (Slave_fake_prepare i)
	    end
	  | Prepare(n',_) ->
	    if n' <= n then
	      begin
		begin
		  if n' <> -1L then
		    let reply = Nak(n',(n,i)) in
		    log ~me "steady state :: replying with %S" (string_of reply) >>= fun () ->
		    send reply me source
		  else Lwt.return ()
		end >>= fun () ->
		Lwt.return (Slave_steady_state state)
	      end
	    else (* n' > n *) 
	      begin
		let reply = Promise(n',i,None) in
		log ~me "steady state :: replying with %S" (string_of reply) >>= fun () ->
		send reply me source >>= fun () ->
		let maybe_previous = Some (previous, Sn.pred i) in
		Lwt.return (Slave_wait_for_accept (n', i, None, maybe_previous))
	      end
	  | Nak (n',(n'',i'')) ->
	    begin
	      log ~me "steady state :: dropping %s" (string_of msg) >>= fun () ->
	      Lwt.return (Slave_steady_state state)
	    end
	  | Promise _ ->
	    begin
	      log ~me "steady state :: dropping %s" (string_of msg) >>= fun () ->
	      Lwt.return (Slave_steady_state state)
	    end  
	  | Accepted(n',i') ->
	    begin
	      log ~me "steady state :: dropping %s" (string_of msg) >>= fun () ->
	      Lwt.return (Slave_steady_state state)
	    end
      end
    | ElectionTimeout n' ->
      log ~me "steady state :: ignoring election timeout" >>= fun () ->
      Lwt.return (Slave_steady_state state)
    | LeaseExpired n' -> 
      let ns  = (Sn.string_of n) 
      and ns' = (Sn.string_of n') in
      if (not (is_election constants)) || n' < n
      then 
	begin
	  log ~me "steady state: ignoring old lease expiration (n'=%s,n=%s)" ns' ns 
	  >>= fun () ->
	  Lwt.return (Slave_steady_state (n,i,previous, true))
	end
      else
	begin 
	  let last_accepted_lease () = 
	    constants.store # who_master() >>= fun maybe_stored ->
	    match maybe_stored with 
              | None -> Lwt.return ( "not_in_store", ("None", Sn.start) )
	      | Some (sm,sd) ->
		let Value.V(update_string) = previous in
		let u,_ = Update.from_buffer update_string 0 in
		match u with
		  | Update.MasterSet (m,d) -> Lwt.return ("previous",(m,d))
		  | _ -> Lwt.return ("stored",(sm,sd))
	  in
	  last_accepted_lease () >>= fun (origine,(am,al)) ->
	  let now = Int64.of_float (Unix.time()) in
	  log ~me "steady state: lease expired(n'=%s) (lease:%s (%s,%s) now=%s" 
	    ns' origine am (Sn.string_of al) (Sn.string_of now)
	  >>= fun () ->
	  let elections_needed = 
	    let diff = Int64.to_int (Int64.sub now al) in
	    diff >= constants.lease_expiration 
	  in
	  if elections_needed then
	    begin
	      log ~me "ELECTIONS NEEDED" >>= fun () ->
	      constants.on_consensus (previous, n,Sn.pred i) >>= fun _ ->
	      let new_n = update_n constants n in
	      Lwt.return (Election_suggest (new_n, i))
	    end
	  else
	    begin
	      start_lease_expiration_thread constants n 
		constants.lease_expiration >>=fun() ->
	      Lwt.return (Slave_steady_state(n,i,previous, true))
	    end
	end
    | FromClient (vo,cb) -> 
      (* there is a window in election 
	 that allows clients to get through before the node became a slave
	 but I know I'm a slave now, so I let the update fail.
      *)      
      let result = Store.Update_fail (Arakoon_exc.E_NOT_MASTER, "Not_Master") in
      cb result >>= fun () ->
      Lwt.return (Slave_steady_state(n,i,previous, true))
      
(* a pending slave that has promised a value to a pending master waits
   for an Accept from the master about this *)
let slave_wait_for_accept constants (n,i, vo, maybe_previous) event =
  match event with 
    | FromNode(msg,source) ->
      begin
	let send = constants.send in
	let me = constants.me in
	log ~me "slave_wait_for_accept n=%s:: received %S from %s" 
	  (Sn.string_of n) (string_of msg) source
	>>= fun () ->
	match msg with
	  | Prepare (n',i') ->
	    constants.on_witness source i' >>= fun () ->
	    if n' <= n then
	      begin
		 let reply = Nak (n',(n,i)) in send reply me source >>= fun () -> 
		log ~me "slave_wait_for_accept: ignoring %S with lower or equal n" (string_of msg)
		>>= fun () ->
		Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
	      end
	    else 
	      begin
		let vo2 = if n' = n then vo else None in
		let reply = Promise (n',i,vo2) in
		send reply me source >>= fun () ->
		log ~me "slave_wait_for_accept: sent %s" (string_of reply) 
		>>= fun () ->
		Lwt.return (Slave_wait_for_accept (n',i,vo2, maybe_previous))
	      end
	  | Accept (n',i',v) when (n',i')=(n,i) ->
	    begin
	      constants.on_witness source i' >>= fun () ->
	      constants.on_accept (v,n,i) >>= fun () ->
	      constants.on_consensus (v,n,i) >>= fun _ ->
	      let reply = Accepted(n,i) in
	      log ~me "replying with %S" (string_of reply) >>= fun () ->
	      send reply me source >>= fun () -> 
	      (* TODO: should assert we really have a MasterSet here *)
	      begin 
		if is_election constants then
		  start_lease_expiration_thread constants n 
		    constants.lease_expiration
		else Lwt.return () 
	      end
	      >>= fun () ->
	      Lwt.return (Slave_steady_state (n, Sn.succ i, v, false))
	    end
	  | Accept (n',i',v) when n' < n ->
	    begin
	      log ~me "slave_wait_for_accept: dropping old accept: %s " (string_of msg) 
	      >>= fun () ->
	      Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
	    end
	  | Accept (n',i',v) ->
	    begin
	      log ~me "slave_wait_for_accept : foreign(%s,%s) <> (%s,%s) sending fake prepare" 
		(Sn.string_of n') (Sn.string_of i') (Sn.string_of n) (Sn.string_of i) 
	      >>= fun () ->
	      Lwt.return (Slave_fake_prepare i)
	    end
	  | Promise(n',i',vo') ->
	    begin
	      log ~me "dropping: %s " (string_of msg) >>= fun () ->
	      Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
	    end
	  | Nak (n',(n2,i2)) ->
	    if n' < n then
	      begin
		log ~me "ignoring old %s " (string_of msg) >>= fun () ->
		Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
	      end
	    else
	      paxos_fatal me "slave_wait_for_accept: received unexpected %S: FATAL" 
		(string_of msg)
	  | Accepted _ ->
	    begin
	      log ~me "dropping old %S " (string_of msg) >>= fun () ->
	      Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
	    end
      end
    | _ -> paxos_fatal constants.me "slave_wait_for_accept only registered for FromNode"

(* a pending slave that discovered another master has to do
   catchup and then go to steady state or wait_for_accept
   depending on if there was an existing value or not *)

let slave_discovered_other_master constants state () =
  let (master, current_i, future_n, future_i) = state in
  let me = constants.me
  and other_cfgs = constants.other_cfgs
  and store = constants.store
  and tlog_coll = constants.tlog_coll
  in
  if current_i < future_i then
    begin
      log ~me "slave_discovered_other_master: catching up from %s" master >>= fun () ->
      Catchup.catchup me other_cfgs (store, tlog_coll)
	current_i master (future_n, future_i) >>= fun (future_n', current_i', vo') ->
      (* start up lease expiration *) 
      start_lease_expiration_thread constants future_n' constants.lease_expiration >>= fun () ->
      begin
	let fake = Prepare( Sn.of_int (-2), (* make it completely harmless *)
			    Sn.pred current_i') (* pred =  consensus_i *)
	in
	Multi_paxos.mcast constants fake >>= fun () ->
	
	match vo' with
	  | Some v -> Lwt.return (Slave_steady_state (future_n', current_i', v, true))
	  | None -> Lwt.return (Slave_wait_for_accept (future_n', current_i', None, None))
      end
    end
  else if current_i = future_i then
    begin
      log ~me "slave_discovered_other_master: no need for catchup %s" master >>= fun () ->
      Lwt.return (Slave_wait_for_accept (future_n, current_i, None, None))
    end
  else
    begin
      log ~me "slave_discovered_other_master: my i is bigger then theirs ; back to election" >>= fun () ->
      (* we have to go to election here or we can get in a situation where
         everybody just waits for each other *)
      Lwt.return (Election_suggest (future_n, current_i))
    end

