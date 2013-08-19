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
    then [EStartElectionTimeout (current_n, current_i)]
    else []
  end in
  let mcast_e = EMCast fake in
  let sides = s @ [log_e;mcast_e] in
  Fsm.return ~sides:sides (Slave_waiting_for_prepare (current_i,current_n))

(* a pending slave that is in sync on i and is ready
   to receive accepts *)
let slave_steady_state (type s) constants state event =
  let (n,i,maybe_previous) = state in
  let store = constants.store in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  let handle_timeout n' i' =
    if (not (is_election constants)) || n' < n || i' < i
    then
      begin
        let ns = (Sn.string_of n) and
          ns' = (Sn.string_of n') in
        let log_e = ELog (fun () ->
            Printf.sprintf "slave_wait_for_accept: Ingoring old lease expiration (n'=%s n=%s i'=%s)" ns' ns (Sn.string_of i'))
        in
        Fsm.return ~sides:[log_e] (Slave_steady_state state)
      end
    else if constants.is_learner
    then
      let ns = (Sn.string_of n) in
      let log_e = ELog (fun () ->
          Printf.sprintf "steady_state: ignoring lease expiration because I am a learner (n=%s)" ns)
      in
      start_lease_expiration_thread constants n constants.lease_expiration >>= fun () ->
      Fsm.return ~sides:[log_e] (Slave_steady_state (n,i,maybe_previous))
    else
      let elections_needed,_ = time_for_elections constants n' maybe_previous in
      if elections_needed then
        begin
          let log_e = ELog (fun () -> "slave_wait_for_accept: Elections needed") in
          let el_i = S.get_succ_store_i constants.store in
          let el_up = constants.get_value el_i in (* TODO election_suggest should figure this out *)
          let new_n = update_n constants n in
          Fsm.return ~sides:[log_e] (Election_suggest (new_n, el_i, el_up))
        end
      else
        begin
          start_lease_expiration_thread constants n constants.lease_expiration >>= fun () ->
          Fsm.return (Slave_steady_state state)
        end
  in
  match event with
    | FromNode (msg,source) ->
      begin
        let log_e0 = ELog (fun () ->
            Printf.sprintf "slave_steady_state n:%s i:%s got %S from %s"
              (Sn.string_of n) (Sn.string_of i) (string_of msg) source
          )
        in
        let accept_value i' v msg =
          let reply = Accepted(n,i') in
          let accept_e = EAccept (v,n,i') in
          let start_e = EStartLeaseExpiration(v,n, true) in
          let send_e = ESend(reply, source) in
          let log_e = ELog (fun () ->
              Printf.sprintf msg (string_of reply)
            )
          in
          let sides = [log_e0;accept_e;start_e; send_e; log_e] in
          Fsm.return ~sides (Slave_steady_state (n, Sn.succ i', Some v))
        in
        match msg with
          | Accept (n',i',v) when (n',i') = (n,i) ->
            begin
              begin
                (* we should have either a previous value received for this n
                     (from same master) in that case store_i == i - 2
                   or we should have no previous value and store_i = i - 1 *)
                let store_i = match S.consensus_i store with
                  | None -> Sn.pred Sn.start
                  | Some store_i -> store_i in
                begin
                  match maybe_previous with
                    | None ->
                      if Sn.sub i store_i = 1L
                      then
                        Logger.debug_f_ "%s: slave: no previous, so not pushing anything" constants.me
                      else
                        paxos_fatal constants.me "slave: no previous, mismatch store_i = %Li, i = %Li" store_i i
                    | Some previous ->
                      if Sn.sub i store_i = 2L
                      then
                        begin
                          Logger.debug_f_ "%s: slave: have previous, so that implies consensus" constants.me >>= fun () ->
                          constants.on_consensus (previous, n, Sn.pred i) >>= fun _ ->
                          Lwt.return_unit
                        end
                      else
                        paxos_fatal constants.me "slave: with previous, mismatch store_i = %Li, i = %Li" store_i i
                end
              end
              >>= fun _ ->
              accept_value i' v "steady_state :: replying with %S"
            end
          | Accept (n',i',v) when (n',Sn.succ i') = (n,i) ->
            let store_i = match S.consensus_i store with
              | None -> Sn.pred Sn.start
              | Some store_i -> store_i in
            if Sn.sub i' store_i = 0L
            then
              begin
                (* already learned this value, so no longer replying with accepted *)
                Logger.debug_f_ "%s: slave, received old accepted (latest applied to store), ignoring" constants.me >>= fun () ->
                Fsm.return (Slave_steady_state state)
              end
            else
              accept_value i' v "steady_state :: replying again to previous with %S"
          | Accept (n',i',v) when
              (n'<=n && i'<i) || (n'< n && i'=i)  ->
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
                Fsm.return (Slave_steady_state (n', next_i, None))
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
    | ElectionTimeout (n', i') ->
      handle_timeout n' i'
    | LeaseExpired n' ->
      handle_timeout n' i
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
        Fsm.return  (Slave_steady_state state)
      end
    | Quiesce (sleep,awake) ->
      handle_quiesce_request (module S) constants.store sleep awake >>= fun () ->
      Fsm.return (Slave_steady_state state)
    | Unquiesce ->
      handle_unquiesce_request constants n >>= fun (store_i, vo) ->
      Fsm.return  (Slave_steady_state state)
    | DropMaster (sleep, awake) ->
      Multi_paxos.safe_wakeup sleep awake () >>= fun () ->
      Fsm.return (Slave_steady_state state)


(* a pending slave that discovered another master has to do
   catchup and then go to steady state *)
let slave_discovered_other_master (type s) constants state () =
  let (master, current_i, future_n, future_i) = state in
  let me = constants.me
  and other_cfgs = constants.other_cfgs
  and store = constants.store
  and tlog_coll = constants.tlog_coll
  in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  if current_i <= future_i then
    begin
      Logger.debug_f_
        "%s: slave_discovered_other_master: catching up from %s @ %s"
        me master (Sn.string_of future_i) >>= fun() ->
      let cluster_id = constants.cluster_id in
      Catchup.catchup ~stop:constants.stop me other_cfgs ~cluster_id ((module S), store, tlog_coll) current_i master future_n
      >>= fun () ->
      begin
        let current_i' = S.get_succ_store_i store in
        let fake = Prepare( Sn.of_int (-2), (* make it completely harmless *)
                            Sn.pred current_i') (* pred =  consensus_i *)
        in
        Multi_paxos.mcast constants fake >>= fun () ->

        start_lease_expiration_thread constants future_n constants.lease_expiration >>= fun () ->
        Fsm.return (Slave_steady_state (future_n, current_i', None));
      end
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
            Slave_steady_state( future_n, next_i, None ),
            "slave_discovered_other_master: forced slave, back to slave mode"
          end
      in
      let log_e = ELog (fun () -> m) in
      Fsm.return ~sides:[log_e] s
    end
