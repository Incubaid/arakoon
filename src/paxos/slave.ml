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

let time_for_elections constants n' maybe_previous =
  begin
    if constants.store # quiesced () 
    then false
    else
      begin
	    let origine,(am,al) = 
          match constants.store # who_master() with
		    | None         -> "not_in_store", ("None", Sn.start) 
		    | Some (sm,sd) -> "stored", (sm,sd) 
	    in
	    let now = Int64.of_float (Unix.time()) in
	    (* 
           let ns' = Sn.string_of n' in
           Lwt_log.debug_f "time_for_elections: lease expired(n'=%s) (lease:%s (%s,%s) now=%s"
		  ns' origine am (Sn.string_of al) (Sn.string_of now) >>= fun () -> 
        *)
	    let diff = abs (Int64.to_int (Int64.sub now al)) in
	    diff >= constants.lease_expiration
      end
  end

(* a forced slave or someone who is outbidded sends a fake prepare
   in order to receive detailed info from the others in a Nak msg *)
let slave_fake_prepare constants (current_i,current_n) () =
  (* fake a prepare, and hopefully wake up a dormant master *)
  let me = constants.me in
  log ~me "slave_fake_prepare: sending Prepare(-1)" >>= fun () ->
  let fake = Prepare( Sn.of_int (-1), current_i) in
  Multi_paxos.mcast constants fake >>= fun () ->
  Lwt.return (Slave_waiting_for_prepare (current_i,current_n))

(* a pending slave that is in sync on i and is ready
   to receive accepts *)
let slave_steady_state constants state event =
  let (n,i,previous) = state in
  let me = constants.me in
  let store = constants.store in
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
		        let m_store_i = store # consensus_i () in
		        begin
		          match m_store_i with
		            | None -> constants.on_consensus (previous, n, Sn.pred i)
		            | Some store_i ->
                      let prev_i = Sn.pred i in
                      if (Sn.compare store_i (Sn.pred prev_i) ) == 0
                      then
			            constants.on_consensus (previous, n,prev_i) 
                      else
			            if Sn.compare store_i prev_i == 0
			            then 
			              Lwt_log.debug_f "Preventing re-push of : %s. Store at %s" (Sn.string_of prev_i) (Sn.string_of store_i) >>= fun () -> 
                      Lwt.return (Store.Ok None)
			            else
			              Llio.lwt_failfmt "Illegal push requested: %s. Store at %s" (Sn.string_of prev_i) (Sn.string_of store_i)      
	            end 
              end >>= fun _ ->
	          constants.on_accept(v,n,i) >>= fun v ->
              begin
		        if Update.is_master_set v 
		        then start_lease_expiration_thread constants n constants.lease_expiration
		        else Lwt.return () 
              end >>= fun () ->
              log ~me "steady_state :: replying with %S" (string_of reply) 
	          >>= fun () ->
	          send reply me source >>= fun () ->
	          Lwt.return (Slave_steady_state (n, Sn.succ i, v))
	        end
	      | Accept (n',i',v) when 
              (n'<=n && i'<i) || (n'< n && i'=i)  ->
	        begin
	          log ~me "slave_steady_state received old %S for my n, ignoring" 
		        (string_of msg) >>= fun () ->
	          Lwt.return (Slave_steady_state state)
	        end
	      | Accept (n',i',v) ->
	        begin
	          log ~me "slave_steady_state foreign (%s,%s) from %s <> local (%s,%s) discovered other master"
		        (Sn.string_of n') (Sn.string_of i') source (Sn.string_of  n) (Sn.string_of  i)
	          >>= fun () ->
              Store.get_catchup_start_i constants.store >>= fun cu_pred ->
              let new_state = (source,cu_pred,n',i') in 
              Lwt.return (Slave_discovered_other_master(new_state) ) 
	        end
	      | Prepare(n',i') ->
	        begin
              handle_prepare constants source n n' i' >>= function
		        | Prepare_dropped 
		        | Nak_sent ->
		          Lwt.return (Slave_steady_state state)
		        | Promise_sent_up2date ->
		          Store.get_succ_store_i constants.store >>= fun next_i ->
		          Lwt.return (Slave_wait_for_accept (n', next_i, None, None))
		            | Promise_sent_needs_catchup ->
		              Store.get_catchup_start_i constants.store >>= fun i ->
		              let new_state = (source, i, n', i') in 
		              Lwt.return (Slave_discovered_other_master(new_state) ) 
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
      begin
        log ~me "steady state :: ignoring election timeout" >>= fun () ->
        Lwt.return (Slave_steady_state state)
      end
    | LeaseExpired n' -> 
      let ns  = (Sn.string_of n) 
      and ns' = (Sn.string_of n') in
      if (not (is_election constants || constants.is_learner)) || n' < n
      then 
	    begin
	      log ~me "steady state: ignoring old lease expiration (n'=%s,n=%s)" ns' ns 
	      >>= fun () ->
	      Lwt.return (Slave_steady_state (n,i,previous))
	    end
      else
	    begin 
	      let elections_needed = time_for_elections constants n' (Some (previous,Sn.pred i)) in
	      if elections_needed then
	        begin
	          log ~me "ELECTIONS NEEDED" >>= fun () ->
	          let new_n = update_n constants n in
              Store.get_succ_store_i constants.store >>= fun el_i ->
              let el_up =
		        begin
		          if el_i = (Sn.pred i) 
		          then Some previous
		          else None
		        end
              in
	          Lwt.return (Election_suggest (new_n, el_i, el_up ))
	        end
	      else
	        begin
	          Lwt.return (Slave_steady_state(n,i,previous))
	        end
	    end
    | FromClient (vo,cb) -> 
      begin
      (* there is a window in election 
	     that allows clients to get through before the node became a slave
	     but I know I'm a slave now, so I let the update fail.
      *)      
        let result = Store.Update_fail (Arakoon_exc.E_NOT_MASTER, "Not_Master") in
        cb result >>= fun () ->
        Lwt.return (Slave_steady_state(n,i,previous))
      end
    | Quiesce (sleep,awake) ->
      begin
        handle_quiesce_request constants.store sleep awake >>= fun () ->
        Lwt.return (Slave_steady_state state)
      end
      
    | Unquiesce ->
      begin
        handle_unquiesce_request constants n >>= fun (store_i, vo) ->
        Lwt.return (Slave_steady_state state)
      end
      
(* a pending slave that has promised a value to a pending master waits
   for an Accept from the master about this *)
let slave_wait_for_accept constants (n,i, vo, maybe_previous) event =
  let me = constants.me in
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
            begin
	          constants.on_witness source i' >>= fun () ->
              handle_prepare constants source n n' i' >>= function
                | Prepare_dropped -> Lwt.return( Slave_wait_for_accept (n,i,vo, maybe_previous) )
                | Nak_sent -> Lwt.return( Slave_wait_for_accept (n,i,vo, maybe_previous) )
                | Promise_sent_up2date -> Lwt.return( Slave_wait_for_accept (n',i,vo, maybe_previous) )
                | Promise_sent_needs_catchup -> 
                  Store.get_catchup_start_i constants.store >>= fun i ->
                  let state = (source, i, n', i') in 
                  Lwt.return( Slave_discovered_other_master state )
            end
          | Accept (n',i',v) when n'=n ->
            begin
              constants.on_witness source i' >>= fun () ->
              let tlog_coll = constants.tlog_coll in
              let tlc_i = tlog_coll # get_last_i () in
              if i' < tlc_i 
              then 
                log ~me "slave_wait_for_accept: dropping old accept (i=%s , i'=%s)" (Sn.string_of i) (Sn.string_of i') >>= fun () ->
              Lwt.return (Slave_wait_for_accept (n, i, vo, maybe_previous))
              else
                begin
	              if i' > i then 
                    Store.get_catchup_start_i constants.store >>= fun cu_pred ->
                  Lwt.return( Slave_discovered_other_master(source, cu_pred, n', i') )   
                  else
                    begin
	                  constants.on_accept (v,n,i') >>= fun v ->
                      begin
		                if Update.is_master_set v 
		                then start_lease_expiration_thread constants n constants.lease_expiration 
		                else Lwt.return ()
	                  end >>= fun () ->
                      match maybe_previous with
		                | None -> begin log ~me "No previous" >>= fun () -> Lwt.return() end
		                | Some( pv, pi ) -> 
                          let store_i = constants.store # consensus_i () in
                          begin
		                    match store_i with
		                      | Some s_i ->
			                    if (Sn.compare s_i pi) == 0 
			                    then Lwt_log.debug "slave_wait_for_accept: Not pushing previous"
			                    else 
			                      begin
			                        Lwt_log.debug_f "slave_wait_for_accept: Pushing previous (%s %s)" 
			                          (Sn.string_of s_i) (Sn.string_of pi) >>=fun () ->
			                        constants.on_consensus(pv,n,pi) >>= fun _ ->
			                        Lwt.return ()
			                      end
                              | None -> constants.on_consensus(pv,n,pi) >>= fun _ -> Lwt.return()
                          end
                    end >>= fun _ ->
	              let reply = Accepted(n,i') in
	              log ~me "replying with %S" (string_of reply) >>= fun () ->
	              send reply me source >>= fun () -> 
	              (* TODO: should assert we really have a MasterSet here *)
	              Lwt.return (Slave_steady_state (n, Sn.succ i', v))
	            end
            end
          | Accept (n',i',v) when n' < n ->
            begin
              if i' > i 
              then
                log ~me "slave_wait_for_accept: Got accept from other master with higher i (i: %s , i' %s)"
                  (Sn.string_of i) (Sn.string_of i')  
                 >>= fun () -> 
              Store.get_catchup_start_i constants.store >>= fun cu_pred ->
              let new_state = (source, cu_pred, n', i') in 
              Lwt.return (Slave_discovered_other_master(new_state) ) 
              else
	            log ~me "slave_wait_for_accept: dropping old accept: %s " (string_of msg) 
	             >>= fun () ->
	          Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
	        end
	      | Accept (n',i',v) ->
	        begin
	          log ~me "slave_wait_for_accept : foreign(%s,%s) <> (%s,%s) sending fake prepare" 
		        (Sn.string_of n') (Sn.string_of i') (Sn.string_of n) (Sn.string_of i) 
	          >>= fun () ->
	          Lwt.return (Slave_fake_prepare (i,n'))
	        end
	      | Promise(n',i',vo') ->
	        begin
	          log ~me "dropping: %s " (string_of msg) >>= fun () ->
	          Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
	        end
	      | Nak (n',(n2,i2)) ->
	        begin
		      log ~me "ignoring %s " (string_of msg) >>= fun () ->
		      Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
	        end
	      | Accepted _ ->
	        begin
	          log ~me "dropping old %S " (string_of msg) >>= fun () ->
	          Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
	        end
      end
    | ElectionTimeout n' 
    | LeaseExpired n' ->
      if (not (is_election constants || constants.is_learner)) || n' < n
      then 
        begin
        let ns = (Sn.string_of n) and
        ns' = (Sn.string_of n') in
        log ~me "slave_wait_for_accept: Ingoring old lease expiration (n'=%s n=%s)" ns' ns >>= fun () ->
        Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
        end
      else
        let elections_needed = time_for_elections constants n' maybe_previous in
        if elections_needed then
          begin
            log ~me "slave_wait_for_accept: Elections needed" >>= fun () ->
            (* begin *)
            Store.get_succ_store_i constants.store >>= fun el_i ->
            let el_up = constants.get_value el_i in
            (*
              begin
              if el_i = (Sn.pred i) 
              then 
              begin
              match maybe_previous with
              | None -> None
                  | Some ( pup, prev_i )  -> Some pup 
                end
              else None
            end
            in
            
            Lwt.return (el_i,el_up)
            end
            >>= fun (el_i, el_up) ->
            *)
            let new_n = update_n constants n in
            Lwt.return (Election_suggest (new_n, el_i, el_up))
          end
        else
          begin
            Lwt.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
          end
            
    | FromClient msg -> paxos_fatal constants.me "slave_wait_for_accept only registered for FromNode"
      
    | Quiesce (sleep,awake) ->
      begin
        handle_quiesce_request constants.store sleep awake >>= fun () ->
        Lwt.return (Slave_wait_for_accept (n,i, vo, maybe_previous))
      end
    | Unquiesce ->
      handle_unquiesce_request constants n >>= fun (store_i, store_vo) ->
      Lwt.return (Slave_wait_for_accept (n,i, vo, maybe_previous))


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
      log ~me "slave_discovered_other_master: catching up from %s" master >>= fun() ->
      begin
        match tlog_coll # get_last_update current_i with
          | None -> Lwt.return None
          | Some up -> Lwt.return( Some( Update.make_update_value up ))
      end >>= fun m_val ->
      let reply = Promise(future_n, current_i,m_val) in
      constants.send reply me master >>= fun () ->
      let cluster_id = constants.cluster_id in
      Catchup.catchup me other_cfgs ~cluster_id (store, tlog_coll) current_i master (future_n, future_i) 
      >>= fun (future_n', current_i', vo') ->
      begin
	    let fake = Prepare( Sn.of_int (-2), (* make it completely harmless *)
			                Sn.pred current_i') (* pred =  consensus_i *)
	    in
	    Multi_paxos.mcast constants fake >>= fun () ->
	    
	    match vo' with
	      | Some v ->
            begin
              start_lease_expiration_thread constants future_n' constants.lease_expiration >>= fun () -> 
              Lwt.return (Slave_steady_state (future_n', current_i', v))
            end
	      | None -> 
            let vo =
              begin
                match vo' with
                  | None -> None
                  | Some u -> Some ( u, current_i' )
              end in
            start_election_timeout constants future_n >>= fun () ->
            Lwt.return (Slave_wait_for_accept (future_n', current_i', None, vo))
      end
    end
  else if current_i = future_i then
    begin
      log ~me "slave_discovered_other_master: no need for catchup %s" master >>= fun () ->
      let f_i = tlog_coll # get_last_i () in
      let vo = tlog_coll # get_last_update f_i in
      begin
      match vo with 
        | None -> 
          begin
            Lwt_log.debug "slave_discovered_other_master: no previous" 
		    >>= fun () -> Lwt.return None
          end
        | Some u -> Lwt_log.debug_f "slave_discovered_other_master: setting previous to %s" 
	      (Sn.string_of f_i) >>= fun () ->
          Lwt.return (Some ( Update.make_update_value u , f_i ))
      end >>= fun vo' ->
      log ~me "Resending Promise" >>= fun () ->
      let prom_val = constants.get_value future_i in
      let reply = Promise(future_n, future_i, prom_val ) in
      constants.send reply me master >>= fun () ->
      start_election_timeout constants future_n >>= fun () ->
      Lwt.return (Slave_wait_for_accept (future_n, current_i, None, vo'))
    end
  else
    begin
      if is_election constants
      then 
	      log ~me "slave_discovered_other_master: my i is bigger then theirs ; back to election"
	      >>= fun () ->
	      (* we have to go to election here or we can get in a situation where
	         everybody just waits for each other *)
	      let new_n = update_n constants future_n in
	      let store = constants.store in
	      let store_i = store # consensus_i () in
	      let suc_store = 
	        begin
	          match store_i with
	            | None -> Sn.start
	            | Some si -> Sn.succ si
	        end 
          in
	      let tlog_coll = constants.tlog_coll in
	      let l_up = tlog_coll # get_last_update suc_store in
	      let l_up_v =
	        begin 
	          match l_up with
	            | None -> None
	            | Some up -> Some ( Update.make_update_value up )
	        end 
          in 
	      Lwt.return (Election_suggest (new_n, suc_store, l_up_v))
      else
        log ~me "slave_discovered_other_master: forced slave, back to slave mode" >>= fun () ->
      Store.get_succ_store_i constants.store >>= fun next_i ->
      Lwt.return (Slave_wait_for_accept( future_n, next_i, None, None ) )
    end

