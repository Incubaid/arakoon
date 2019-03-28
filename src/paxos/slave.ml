(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

open Multi_paxos_type
open Multi_paxos
open Lwt
open Mp_msg.MPMessage

let time_for_elections ?invalidate_lease_start_until (type s) constants =
  begin
    let module S = (val constants.store_module : Store.STORE with type t = s) in
    if S.quiesced constants.store
    then false, "quiesced"
    else
      begin
        let invalidate_lease_start_until = match invalidate_lease_start_until with
          | Some x -> x
          | None -> Unix.gettimeofday () -. (float constants.lease_expiration) in
        let lease_start =
          match S.who_master constants.store with
          | None         -> 0.0
          | Some (_m,sd) -> sd
        in
        let return need_elections =
          need_elections, Printf.sprintf "%f >= %f" invalidate_lease_start_until lease_start in
        if invalidate_lease_start_until >= lease_start
        then
          begin
            match constants.respect_run_master with
            | None ->
              return true
            | Some(_, until) ->
              if Unix.gettimeofday () < until
              then
                false, "lease expired, but respecting another node running for master"
              else
                return true
          end
        else
          return false
      end
  end

let decline_client_update ufs =
  let fail = Store.Update_fail (Arakoon_exc.E_NOT_MASTER, "Not_Master") in
  join (List.map (fun (_, _, f) -> f fail) ufs)

(* a forced slave or someone who is outbidded sends a fake prepare
   in order to receive detailed info from the others in a Nak msg *)
let slave_fake_prepare constants (current_i,current_n) () =
  (* fake a prepare, and hopefully wake up a dormant master *)
  let log_e = ELog (fun () -> "slave_fake_prepare: sending Prepare(-1)") in
  let fake = Prepare( Sn.of_int (-1), current_i) in
  let mcast_e = EMCast fake in
  let sides =
    if is_election constants
    then [log_e; EStartElectionTimeout (current_n, current_i); mcast_e]
    else [log_e; mcast_e]
  in
  Fsm.return ~sides:sides (Slave_steady_state (current_n, current_i, None))

let handle_timeout ((n,i,_maybe_previous) as state) constants invalidate_lease_start_until n' i' =
  if not (is_election constants)
  then
    begin
      let log_e = ELog (fun () ->
          Printf.sprintf "slave_steady_state: Ignoring old timeout because the master is forced or readonly")
      in
      Fsm.return ~sides:[log_e] (Slave_steady_state state)
    end
  else if n' < n || i' < i
  then
    begin
      let log_e = ELog (fun () ->
          Printf.sprintf "slave_steady_state: Ingoring old timeout (n'=%s n=%s i'=%s i=%s)" (Sn.string_of n') (Sn.string_of n) (Sn.string_of i') (Sn.string_of i))
        in
        Fsm.return ~sides:[log_e] (Slave_steady_state state)
      end
    else
      let elections_needed,why = time_for_elections ?invalidate_lease_start_until constants in
      if elections_needed then
      begin
        let log_e = ELog (fun () ->
            Printf.sprintf "slave_steady_state: Elections needed because %s" why) in
        let new_n = update_n constants n in
        Fsm.return ~sides:[log_e] (Election_suggest (new_n, 0))
      end
    else
      begin
        let log_e = ELog (fun () ->
            Printf.sprintf "slave_steady_state: Elections not needed because %s" why) in
        Fsm.return ~sides:[log_e] (Slave_steady_state state)
      end

let handle_from_node ((n,i,maybe_previous) as state) (type s) (module S : Store.STORE with type t = s) constants msg source  =
  let store = constants.store in
  let log_e0 = ELog (fun () ->
      Printf.sprintf "slave_steady_state n:%s i:%s got %S from %s"
        (Sn.string_of n) (Sn.string_of i) (string_of msg) source
    )
  in
  let accept_value oconsensus n' i' v msg =
    let reply = Accepted(n',i') in
    let accept_e = EAccept (v,n',i') in
    let send_e = ESend(reply, source) in
    let log_e = ELog (fun () ->
        Printf.sprintf msg (string_of reply)
      )
    in
    let sides = log_e0::accept_e::send_e::log_e::oconsensus in
    Fsm.return ~sides (Slave_steady_state (n', Sn.succ i', Some v))
  in
  let get_store_i () = match S.consensus_i store with
    | None -> Sn.pred Sn.start
    | Some store_i -> store_i in
  match msg with
    | Accept (n',i',v) when (n',i') = (n,i) ->
      begin
        let () = constants.on_witness source i' in
        begin
          (* we should have either a previous value received for this n
               (from same master) in that case store_i == i - 2
             or we should have no previous value and store_i = i - 1 *)
          let store_i = get_store_i () in
          begin
            match maybe_previous with
              | None ->
                if Sn.sub i store_i = 1L
                then
                  Logger.debug_f_ "%s: slave: no previous, so not pushing anything" constants.me >>= fun () ->
                  Lwt.return []
                else
                  paxos_fatal constants.me "slave: no previous, mismatch store_i = %Li, i = %Li" store_i i
              | Some previous ->
                if Sn.sub i store_i = 2L
                then
                  begin
                    Logger.debug_f_ "%s: slave: have previous, so that implies consensus" constants.me >>= fun () ->
                    Lwt.return [EConsensus (None, previous,n,Sn.pred i)]
                  end
                else
                  paxos_fatal constants.me "slave: with previous, mismatch store_i = %Li, i = %Li" store_i i
          end
        end
        >>= fun oconsensus ->
        accept_value oconsensus n' i' v "steady_state :: replying with %S"
      end
    | Accept (n',i',v) when (Sn.succ i' = i) && (n' >= n) ->
      if i' = get_store_i ()
      then
        begin
          (* already learned this value, so no longer writing it to the tlog,
             but we can be polite and answer with accepted again, as it should
             be the same paxos-value as before *)
          Logger.debug_f_ "%s: slave, received old accepted (latest applied to store), ignoring" constants.me >>= fun () ->
          let reply = Accepted(n',i') in
          let send_e = ESend(reply, source) in
          Fsm.return ~sides:[ send_e ] (Slave_steady_state (n', i, maybe_previous))
        end
      else
        accept_value [] n' i' v "steady_state :: replying again to previous with %S"
    | Accept (n',i',_) when i' > i || (i'=i && n' > n) ->
                            (* TODO make helper function to check this! *)
       begin
         let log_e = ELog (fun () ->
                           Printf.sprintf
                             "slave_steady_state foreign (%s,%s) from %s <> local (%s,%s) discovered other master"
                             (Sn.string_of n') (Sn.string_of i') source (Sn.string_of  n) (Sn.string_of  i)
                          )
         in
         let new_state = (source, n', i') in
         Fsm.return ~sides:[log_e0;log_e] (Slave_discovered_other_master new_state)
       end
    | Accept (_,_,_) ->
      begin
        let log_e = ELog (
            fun () ->
              Printf.sprintf "slave_steady_state received old %S for my i, ignoring"
                (string_of msg) )
        in
        Fsm.return ~sides:[log_e0;log_e] (Slave_steady_state state)
      end
    | Prepare(n',i') ->
      begin
        handle_prepare constants source n n' i' >>= function
        | Prepare_dropped
        | Nak_sent ->
          Fsm.return ~sides:[log_e0] (Slave_steady_state state)
        | Promise_sent_up2date ->
          let next_i = S.get_succ_store_i constants.store in
          start_lease_expiration_thread constants >>= fun () ->
          Fsm.return (Slave_steady_state (n', next_i, None))
        | Promise_sent_needs_catchup ->
          let new_state = (source, n', i') in
          Fsm.return ~sides:[log_e0] (Slave_discovered_other_master new_state)
      end
    | Nak(_,(n2, i2)) when i2 > i ->
       begin
         Logger.debug_f_ "%s: got %s => go to catchup" constants.me (string_of msg) >>= fun () ->
         Fsm.return (Slave_discovered_other_master (source, n2, i2))
       end
    | Nak _
    | Promise _
    | Accepted _ ->
      let log_e = ELog (fun () ->
          Printf.sprintf "steady state :: dropping %s" (string_of msg)
        )
      in
      Fsm.return ~sides:[log_e0;log_e] (Slave_steady_state state)

let common_steady_state (type s) handle_from_node handle_timeout next constants state event =
  let (n,i,_maybe_previous) = state in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  match event with
    | FromNode (msg,source) ->
      handle_from_node state (module S : Store.STORE with type t = s) constants msg source
    | ElectionTimeout (n', i') ->
      handle_timeout state constants None n' i'
    | LeaseExpired (lease_start) ->
      handle_timeout state constants (Some lease_start) n i
    | FromClient ufs ->
      begin
        (* there is a window in election
           that allows clients to get through before the node became a slave
           but I know I'm a slave now, so I let the update fail.
        *)
        decline_client_update ufs >>= fun () ->
        Fsm.return (next state)
      end
    | Quiesce (mode, sleep,awake) ->
      handle_quiesce_request (module S) constants.store mode sleep awake >>= fun () ->
      Fsm.return (next state)
    | Unquiesce ->
      handle_unquiesce_request constants >>= fun () ->
      Fsm.return  (next state)
    | DropMaster (sleep, awake) ->
      Multi_paxos.safe_wakeup sleep awake () >>= fun () ->
      Fsm.return (next state)

(* a pending slave that is in sync on i and is ready
   to receive accepts *)
let slave_steady_state (type _s) constants state event =
  common_steady_state handle_from_node handle_timeout (fun x -> Slave_steady_state x)
  constants state event

(* a pending slave that discovered another master has to do
   catchup and then go to steady state *)
let slave_discovered_other_master (type s) constants state () =
  let (master, future_n, future_i) = state in
  let me = constants.me
  and other_cfgs = constants.other_cfgs
  and store = constants.store
  and tlog_coll = constants.tlog_coll
  in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  Logger.debug_f_
    "%s: slave_discovered_other_master: catching up from %s @ %s"
    me master (Sn.string_of future_i) >>= fun() ->
  let cluster_id = constants.cluster_id in
  let tls_ctx = constants.catchup_tls_ctx in
  let master_before = S.who_master store in
  let lease_expired = match master_before with
    | None -> true
    | Some (_, ls) -> ls +. (float_of_int constants.lease_expiration) <= Unix.gettimeofday () in
  Catchup.catchup
    ~tls_ctx
    ~tcp_keepalive:constants.tcp_keepalive
    ~stop:constants.stop
    me other_cfgs ~cluster_id ((module S), store, tlog_coll) master >>= fun () ->

  let master_after = S.who_master store in
  let immediate_lease_expiration =
    (* lease was previously expired and learned no new master leases *)
    lease_expired && (master_after = master_before)
  in
  let current_i' = S.get_succ_store_i store in
  let fake = Prepare( Sn.of_int (-2), (* make it completely harmless *)
                      Sn.pred current_i') (* pred =  consensus_i *)
  in
  Multi_paxos.mcast constants fake >>= fun () ->

  start_lease_expiration_thread ~immediate_lease_expiration constants >>= fun () ->
  Fsm.return (Slave_steady_state (future_n, current_i', None));
