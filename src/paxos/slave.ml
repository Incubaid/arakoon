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

let time_for_elections (type s) constants n' maybe_previous =
  begin
    let module S = (val constants.store_module : Store.STORE with type t = s) in
    if S.quiesced constants.store
    then false, "quiesced"
    else
      begin
	    let origine,(am,al) =
          match S.who_master constants.store with
		    | None         -> "not_in_store", ("None", Sn.start)
		    | Some (sm,sd) -> "stored", (sm,sd)
	    in
	    let now = Unix.gettimeofday () in
	    (*
           let ns' = Sn.string_of n' in
           log "time_for_elections: lease expired(n'=%s) (lease:%s (%s,%s) now=%s"
		  ns' origine am (Sn.string_of al) (Sn.string_of now) >>= fun () ->
        *)
            let alf = Int64.to_float al in
	    let diff = now -. alf in
	    diff >= (float constants.lease_expiration), Printf.sprintf "diff=%f" diff
      end
  end

(* a forced slave or someone who is outbidded sends a fake prepare
   in order to receive detailed info from the others in a Nak msg *)
let slave_fake_prepare constants (current_i,current_n) () =
  (* fake a prepare, and hopefully wake up a dormant master *)
  let log_e = ELog (fun () -> "slave_fake_prepare: sending Prepare(-1)") in
  let fake = Prepare( Sn.of_int (-1), current_i) in

  let s = begin
    if is_election constants
    then [EStartElectionTimeout current_n]
    else []
  end in
  let mcast_e = EMCast fake in
  let sides = s @ [log_e;mcast_e] in
  Fsm.return ~sides:sides (Slave_waiting_for_prepare (current_i,current_n))

(* a pending slave that is in sync on i and is ready
   to receive accepts *)
let slave_steady_state (type s) constants state event =
  let (n,i,previous) = state in
  let store = constants.store in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  match event with
    | FromNode (msg,source) ->
        begin
          let log_e0 =
            ELog (fun () ->
                  Printf.sprintf "slave_steady_state n:%s i:%s got %S from %s"
                                 (Sn.string_of n) (Sn.string_of i) (string_of msg) source)
          in
          match msg with
          | Accept (n',i',v) when (n',i') = (n,i) ->
             begin
               let reply = Accepted(n,i) in
               begin
                 let m_store_i = S.consensus_i store in
                 begin
                   match m_store_i with
                   | None ->
                      Lwt.return [EConsensus(None, previous, n, Sn.pred i)]
                   | Some store_i ->
                      let prev_i = Sn.pred i in
                      if (Sn.compare store_i (Sn.pred prev_i) ) == 0
                      then
                        Lwt.return [EConsensus(None, previous, n, prev_i)]
                      else
                        if Sn.compare store_i prev_i == 0
                        then
                          begin
                            Logger.debug_f_ "%s: Preventing re-push of : %s. Store at %s" constants.me (Sn.string_of prev_i) (Sn.string_of store_i) >>= fun () ->
                            Lwt.return []
                          end
                        else
                          Llio.lwt_failfmt "Illegal push requested: %s. Store at %s" (Sn.string_of prev_i) (Sn.string_of store_i)
                 end
                  end
                  >>= fun consensus_e ->
                  let accept_e = EAccept (v,n,i) in
                  let start_e = EStartLeaseExpiration(v,n, true) in
                  let send_e = ESend(reply, source) in
                  let log_e = ELog (fun () ->
                    Printf.sprintf "steady_state :: replying with %S" (string_of reply)
                  )
                  in
                  let sides = log_e0::accept_e::start_e::send_e::log_e::consensus_e in
                  Fsm.return ~sides (Slave_steady_state (n, Sn.succ i, v))
	            end
          | Accept (n',i',v) when
                 (i'<i) || (n'< n && i'=i)  ->
             begin
                let log_e = ELog (
                  fun () ->
                    Printf.sprintf "slave_steady_state received old %S for my n, ignoring"
		          (string_of msg) )
                in
	            Fsm.return ~sides:[log_e0;log_e] (Slave_steady_state state)
	          end
	      | Accept (n',i',v) ->
	          begin
                let log_e = ELog (fun () ->
	              Printf.sprintf
                    "slave_steady_state foreign (%s,%s) from %s <> local (%s,%s) discovered other master"
		            (Sn.string_of n') (Sn.string_of i') source (Sn.string_of  n) (Sn.string_of  i)
                )
                in
                let cu_pred = S.get_catchup_start_i constants.store in
                let new_state = (source,cu_pred,n',i') in
                Fsm.return ~sides:[log_e0;log_e] (Slave_discovered_other_master(new_state) )
	          end
	      | Prepare(n',i') ->
	          begin
                handle_prepare constants source n n' i' >>= function
		          | Prepare_dropped
		          | Nak_sent ->
		              Fsm.return ~sides:[log_e0] (Slave_steady_state state)
		          | Promise_sent_up2date ->
		              let next_i = S.get_succ_store_i constants.store in
		              Fsm.return (Slave_wait_for_accept (n', next_i, None, None))
		          | Promise_sent_needs_catchup ->
		              let i = S.get_catchup_start_i constants.store in
		              let new_state = (source, i, n', i') in
		              Fsm.return ~sides:[log_e0] (Slave_discovered_other_master(new_state) )
	          end
	      | Nak _
	      | Promise _
	      | Accepted _ ->
              let log_e = ELog (fun () ->
                Printf.sprintf "steady state :: dropping %s" (string_of msg)
              )
              in
	          Fsm.return ~sides:[log_e0;log_e] (Slave_steady_state state)

      end
    | ElectionTimeout n' ->
      begin
        let log_e = ELog (fun () -> "steady state :: ignoring election timeout") in
        Fsm.return ~sides:[log_e] (Slave_steady_state state)
      end
    | LeaseExpired n' ->
        let ns  = (Sn.string_of n)
        and ns' = (Sn.string_of n') in
        if (not (is_election constants || constants.is_learner)) || n' < n
        then
	      begin
            let log_e = ELog (fun () ->
	          Printf.sprintf "steady state: ignoring old lease expiration (n'=%s,n=%s)" ns' ns )
            in
	        Fsm.return ~sides:[log_e] (Slave_steady_state (n,i,previous))
	      end
        else
	      begin
	        let elections_needed, msg = time_for_elections constants n' (Some (previous,Sn.pred i)) in
	        if elections_needed then
	          begin
	            let new_n = update_n constants n in
                let el_i = S.get_succ_store_i constants.store in
                let el_up =
		          begin
		            if el_i = (Sn.pred i)
		            then Some previous
		            else None
		          end
                in
                let log_e = ELog (fun () -> "ELECTIONS NEEDED") in
	            Fsm.return ~sides:[log_e] (Election_suggest (new_n, el_i, el_up ))
	          end
	        else
	          begin
                let log_e = ELog(fun () ->
                  Printf.sprintf
                    "slave_steady_state ignoring lease expiration (n'=%s,n=%s) %s" ns' ns msg)
                in
	            Fsm.return ~sides:[log_e] (Slave_steady_state(n,i,previous))
	          end
	      end
    | FromClient ufs ->
        begin

          (* there is a window in election
	         that allows clients to get through before the node became a slave
	         but I know I'm a slave now, so I let the update fail.
          *)
          let updates,finished_funs = List.split ufs in
          let result = Store.Update_fail (Arakoon_exc.E_NOT_MASTER, "Not_Master") in
          let rec loop = function
            | []       -> Lwt.return ()
            | f :: ffs -> f result >>= fun () ->
                loop ffs
          in
          loop finished_funs >>= fun () ->
          Fsm.return  (Slave_steady_state(n,i,previous))
        end
    | Quiesce (mode, sleep,awake) ->
        begin
          handle_quiesce_request (module S) constants.store mode sleep awake >>= fun () ->
          Fsm.return (Slave_steady_state state)
        end

    | Unquiesce ->
        begin
          handle_unquiesce_request constants n >>= fun (store_i, vo) ->
          Fsm.return  (Slave_steady_state state)
        end
    | DropMaster (sleep, awake) ->
        Multi_paxos.safe_wakeup sleep awake () >>= fun () ->
        Fsm.return (Slave_steady_state state)

(* a pending slave that has promised a value to a pending master waits
   for an Accept from the master about this *)
let slave_wait_for_accept (type s) constants (n,i, vo, maybe_previous) event =
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  match event with
    | FromNode(msg,source) ->
      begin
	    let send = constants.send in
	    let me = constants.me in
	    Logger.debug_f_ "%s: slave_wait_for_accept n=%s:: received %S from %s" me
	      (Sn.string_of n) (string_of msg) source
	    >>= fun () ->
	    match msg with
	      | Prepare (n',i') ->
            begin
	          let () = constants.on_witness source i' in
              handle_prepare constants source n n' i' >>= function
                | Prepare_dropped -> Fsm.return( Slave_wait_for_accept (n,i,vo, maybe_previous) )
                | Nak_sent -> Fsm.return( Slave_wait_for_accept (n,i,vo, maybe_previous) )
                | Promise_sent_up2date -> Fsm.return( Slave_wait_for_accept (n',i,vo, maybe_previous) )
                | Promise_sent_needs_catchup ->
                  let i = S.get_catchup_start_i constants.store in
                  let state = (source, i, n', i') in
                  Fsm.return( Slave_discovered_other_master state )
            end
          | Accept (n',i',v) when n'=n ->
            begin
              let () = constants.on_witness source i' in
              let tlog_coll = constants.tlog_coll in
              let tlc_i = tlog_coll # get_last_i () in
              if i' < tlc_i
              then
                begin
                  let log_e = ELog(fun () ->
                    Printf.sprintf "slave_wait_for_accept: dropping old accept (i=%s , i'=%s)"
                    (Sn.string_of i) (Sn.string_of i'))
                  in
                  Fsm.return ~sides:[log_e] (Slave_wait_for_accept (n, i, vo, maybe_previous))
                end
              else
                begin
	              if i' > i
                  then
                    let cu_pred = S.get_catchup_start_i constants.store in
                    Fsm.return( Slave_discovered_other_master(source, cu_pred, n', i') )
                  else
                    begin
	                  constants.on_accept (v,n,i') >>= fun () ->
                      begin
		                if Value.is_master_set v
		                then start_lease_expiration_thread constants n ~slave:true
		                else Lwt.return ()
	                  end >>= fun () ->
                      match maybe_previous with
		                | None -> begin Logger.debug_f_ "%s: No previous" me >>= fun () -> Lwt.return() end
		                | Some( pv, pi ) ->
                          let store_i = S.consensus_i constants.store in
                          begin
                            match store_i with
                            | Some s_i ->
                              if (Sn.compare s_i pi) == 0 || not ((Sn.compare i (Sn.succ pi)) == 0)
                              then
                                Logger.debug_f_ "%s: slave_wait_for_accept: Not pushing previous" me
                              else
                                begin
                                  Logger.debug_f_ "%s: slave_wait_for_accept: Pushing previous (%s %s)" me
                                    (Sn.string_of s_i) (Sn.string_of pi) >>=fun () ->
                                  constants.on_consensus(pv,n,pi) >>= fun _ ->
                                  Lwt.return ()
                                end
                            | None ->
                                (* store is empty, so a previous entry will
                                   have the same i as the current one: don't push *)
                              Logger.debug_f_ "%s: slave_wait_for_accept: pi=%s i=%s"
                                me (Sn.string_of pi) (Sn.string_of i)
                          end
                    end >>= fun _ ->
	              let reply = Accepted(n,i') in
	              Logger.debug_f_ "%s: replying with %S" me (string_of reply) >>= fun () ->
	              send reply me source >>= fun () ->
	              (* TODO: should assert we really have a MasterSet here *)
	              Fsm.return (Slave_steady_state (n, Sn.succ i', v))
	            end
            end
          | Accept (n',i',v) when n' < n ->
            begin
              if i' > i
              then
                let log_e = ELog
                  (fun () ->
                    Printf.sprintf
                      "slave_wait_for_accept: Got accept from other master with higher i (i: %s , i' %s)"
                      (Sn.string_of i) (Sn.string_of i')
                  )
                in
                let cu_pred = S.get_catchup_start_i constants.store in
                let new_state = (source, cu_pred, n', i') in
                Fsm.return ~sides:[log_e] (Slave_discovered_other_master(new_state) )
              else
                let log_e = ELog
                  (fun () ->
	                Printf.sprintf "slave_wait_for_accept: dropping old accept: %s " (string_of msg)
                  )
	            in
	            Fsm.return ~sides:[log_e] (Slave_wait_for_accept (n,i,vo, maybe_previous))
	        end
	      | Accept (n',i',v) ->
	          begin
                let log_e = ELog (fun () ->
                  Printf.sprintf "slave_wait_for_accept : foreign(%s,%s) <> (%s,%s) sending fake prepare"
		            (Sn.string_of n') (Sn.string_of i') (Sn.string_of n) (Sn.string_of i)
                )
                in
	            Fsm.return ~sides:[log_e] (Slave_fake_prepare (i,n'))
	          end
	      | Promise _
          | Nak _
          | Accepted _ ->
	          begin
                let log_e = ELog (fun () -> "dropping : " ^ (string_of msg)) in
	            Fsm.return ~sides:[log_e] (Slave_wait_for_accept (n,i,vo, maybe_previous))
	          end
      end
    | ElectionTimeout n'
    | LeaseExpired n' ->
      if (not (is_election constants || constants.is_learner)) || n' < n
      then
        begin
        let ns = (Sn.string_of n) and
        ns' = (Sn.string_of n') in
        let log_e = ELog (fun () ->
          Printf.sprintf "slave_wait_for_accept: Ingoring old lease expiration (n'=%s n=%s)" ns' ns)
        in
        Fsm.return ~sides:[log_e] (Slave_wait_for_accept (n,i,vo, maybe_previous))
        end
      else
        let elections_needed,_ = time_for_elections constants n' maybe_previous in
        if elections_needed then
          begin
            let log_e = ELog (fun () -> "slave_wait_for_accept: Elections needed") in
            (* begin *)
            let el_i = S.get_succ_store_i constants.store in
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
            Fsm.return ~sides:[log_e] (Election_suggest (new_n, el_i, el_up))
          end
        else
          begin
            start_lease_expiration_thread constants n ~slave:true >>= fun () ->
            Fsm.return (Slave_wait_for_accept (n,i,vo, maybe_previous))
          end

    | FromClient msg -> paxos_fatal constants.me "slave_wait_for_accept only registered for FromNode"

    | Quiesce (mode, sleep,awake) ->
        begin
          handle_quiesce_request (module S) constants.store mode sleep awake >>= fun () ->
          Fsm.return (Slave_wait_for_accept (n,i, vo, maybe_previous))
        end
    | Unquiesce ->
        begin
          handle_unquiesce_request constants n >>= fun (store_i, store_vo) ->
          Fsm.return (Slave_wait_for_accept (n,i, vo, maybe_previous))
        end
    | DropMaster (sleep, awake) ->
        Multi_paxos.safe_wakeup sleep awake () >>= fun () ->
        Fsm.return (Slave_wait_for_accept (n, i, vo, maybe_previous))

(* a pending slave that discovered another master has to do
   catchup and then go to steady state or wait_for_accept
   depending on if there was an existing value or not *)

let slave_discovered_other_master (type s) constants state () =
  let (master, current_i, future_n, future_i) = state in
  let me = constants.me
  and other_cfgs = constants.other_cfgs
  and store = constants.store
  and tlog_coll = constants.tlog_coll
  in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  if current_i < future_i then
    begin
      Logger.debug_f_
        "%s: slave_discovered_other_master: catching up from %s @ %s"
        me master (Sn.string_of future_i) >>= fun() ->
      let m_val = tlog_coll # get_last_value current_i in
      let reply = Promise(future_n, current_i, m_val) in
      constants.send reply me master >>= fun () ->
      let cluster_id = constants.cluster_id in
      Catchup.catchup me other_cfgs ~cluster_id ((module S), store, tlog_coll) master
      >>= fun () ->
      begin
        let current_i' = S.get_succ_store_i store in
        let vo' = tlog_coll # get_last_value current_i' in

	    let fake = Prepare( Sn.of_int (-2), (* make it completely harmless *)
			                Sn.pred current_i') (* pred =  consensus_i *)
	    in
	    Multi_paxos.mcast constants fake >>= fun () ->

	    match vo' with
	      | Some v ->
            begin
              start_lease_expiration_thread constants future_n ~slave:true >>= fun () ->
              Fsm.return (Slave_steady_state (future_n, current_i', v))
            end
	      | None ->
            let vo =
              begin
                match vo' with
                  | None -> None
                  | Some u -> Some ( u, current_i' )
              end in
            start_election_timeout constants future_n >>= fun () ->
            Fsm.return (Slave_wait_for_accept (future_n, current_i', None, vo))
      end
    end
  else if current_i = future_i then
    begin
      let last = tlog_coll # get_last () in
      let prom_val = constants.get_value future_i in
      let reply = Promise(future_n, future_i, prom_val ) in
      let send_e = ESend (reply, master) in
      let start_e = EStartElectionTimeout future_n in
      let log_e = ELog (fun () ->
        Printf.sprintf "slave_discovered_other_master: no need for catchup %s" master )
      in
      Fsm.return ~sides:[send_e;start_e;log_e] (Slave_wait_for_accept (future_n, current_i, None, last))
    end
  else
    begin
      let next_i = S.get_succ_store_i constants.store in
      let s, m =
        if is_election constants
        then
	      (* we have to go to election here or we can get in a situation where
	         everybody just waits for each other *)
	      let new_n = update_n constants future_n in
	      let tlog_coll = constants.tlog_coll in
	      let l_up_v = tlog_coll # get_last_value next_i in
	      (Election_suggest (new_n, next_i, l_up_v)),
          "slave_discovered_other_master: my i is bigger then theirs ; back to election"
      else
        begin
          Slave_wait_for_accept( future_n, next_i, None, None ),
          "slave_discovered_other_master: forced slave, back to slave mode"
        end
      in
      let log_e = ELog (fun () -> m) in
      Fsm.return ~sides:[log_e] s
    end
