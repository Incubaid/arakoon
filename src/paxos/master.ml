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
open Update

let is_empty = function
  | [] -> true
  | _ -> false

(* a (possibly potential) master has found consensus on a value
   first potentially finish of a client request and then on to
   being a stable master *)
let master_consensus (type s) constants {mo;v;n;i; lew} () =
  let con_e = EConsensus(mo, v,n,i) in
  let log_e = ELog (fun () ->
      Printf.sprintf "on_consensus for : %s => %i finished_fs (in master_consensus)"
        (Value.value2s v)
        (match mo with
           | None -> 0
           | Some ffs -> (List.length ffs) ))
  in
  let inject_e = EGen (fun () ->
      match v with
        | (_, Value.Vm _) ->
          let event = Multi_paxos.FromClient [(Update.Nop, fun _ -> Lwt.return ())] in
          Lwt.ignore_result (constants.inject_event event);
          Lwt.return ()
        | (_, Value.Vc _) ->
          begin
            let inject_lease_expired ls =
              let event = Multi_paxos.LeaseExpired (ls) in
              Lwt.ignore_result (constants.inject_event event) in
            let module S = (val constants.store_module : Store.STORE with type t = s) in
            let me = constants.me in
            let () = match S.who_master constants.store with
              | None ->
                inject_lease_expired 0.0
              | Some (m, ls) when m = me ->
                let diff = (Unix.gettimeofday ()) -. ls in
                if diff >= float constants.lease_expiration
                then
                  (* if we get here because a LeaseExpired message is
                     delivered too late (thx Lwt!) then this injection
                     could result in more & more LeaseExpired messages flowing
                     through the state machine. This effect is contained
                     by how the LeaseExpired messages are handled in the
                     stable_master state below.
                  *)
                  inject_lease_expired ls
              | Some (_m, ls) (* when m <> me *) ->
                (* always insert a lease expired after picking up master role
                   from another node. do not compare gettimeofday with when the
                   lease started, as we might have acted earlier than necessary
                   with the lease expiration timeout from the other node *)
                inject_lease_expired ls in
            Lwt.return ()
          end
    )
  in
  if Value.is_other_master_set constants.me v
  then
    (* step down *)
    Multi_paxos.safe_wakeup_all () lew >>= fun () ->
    let sides = [log_e;con_e;ELog (fun () -> "Stepping down to slave state")] in
    Fsm.return ~sides (Slave_steady_state(n, Sn.succ i, None))
  else
    let state = (n,(Sn.succ i), lew) in
    Fsm.return ~sides:[log_e;con_e;inject_e] (Stable_master state)


let stable_master (type s) constants ((n,new_i, lease_expire_waiters) as current_state) ev =
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  match ev with
    | LeaseExpired (ls) ->
      let me = constants.me in
      begin
        let extend ls =
          if not (is_empty lease_expire_waiters)
          then
            begin
              if (Unix.gettimeofday () -. ls) > 2.2 *. (float constants.lease_expiration)
              then
                begin
                  (* nobody is taking over, go to elections *)
                  Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
                  let log_e = ELog (fun () ->
                                    "stable_master: half-lease_expired while doing drop-master but nobody taking over, go to election")
                  in
                  let new_n = update_n constants n in
                  Fsm.return ~sides:[log_e] (Election_suggest (new_n, 0))
                end
              else
                let log_e = ELog (fun () ->
                                  "stable_master: half-lease_expired while doing drop-master, so not renewing lease")
                in
                Fsm.return ~sides:[log_e] (Stable_master current_state)
            end
          else (* if is_empty lease_expire_waiters *)
            let log_e = ELog (fun () -> "stable_master: half-lease_expired: update lease." ) in
            let v = Value.create_master_value constants.tlog_coll new_i me in
            let ms = {mo = None; v;n;i = new_i;lew = []} in
            Fsm.return ~sides:[log_e] (Master_dictate ms)
        in
        match S.who_master constants.store with
        | None ->
           extend 0.0
        | Some (_, ls') ->
           if ls >= ls'
           then
             extend ls
           else
             begin
               let log_e = ELog (fun () -> Printf.sprintf "stable_master: ignoring old lease expiration %f < %f" ls ls') in
               Fsm.return ~sides:[log_e] (Stable_master current_state)
             end
      end
    | FromClient ufs ->
      begin
        let updates, finished_funs = List.split ufs in
        let synced = List.fold_left (fun acc u -> acc || Update.is_synced u) false updates in
        let v = Value.create_client_value constants.tlog_coll new_i updates synced in
        let ms = {mo = Some finished_funs;v;n;i = new_i;
                  lew = lease_expire_waiters}
        in
        Fsm.return (Master_dictate ms)
      end
    | FromNode (msg,source) ->
      begin
        let me = constants.me in
        match msg with
          | Prepare (n',i') ->
            begin
              if am_forced_master constants me
              then
                begin
                  let reply = Nak(n', (n,new_i)) in
                  constants.send reply me source >>= fun () ->
                  if n' > 0L
                  then
                    let new_n = update_n constants n' in
                    Fsm.return (Election_suggest (new_n, 0))
                  else
                    Fsm.return (Stable_master current_state )
                end
              else
                begin
                  let module S = (val constants.store_module : Store.STORE with type t = s) in
                  handle_prepare constants source n n' i' >>= function
                  | Nak_sent
                  | Prepare_dropped -> Fsm.return  (Stable_master current_state )
                  | Promise_sent_up2date ->
                    begin
                      Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
                      start_lease_expiration_thread constants >>= fun () ->
                      Fsm.return (Slave_steady_state (n', new_i, None))
                    end
                  | Promise_sent_needs_catchup ->
                    let i = S.get_catchup_start_i constants.store in
                    Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
                    Fsm.return (Slave_discovered_other_master (source, i, n', i'))
                end
            end
          | Accepted(_n,i) ->
            (* This one is not relevant anymore, but we're interested
               to see the slower slaves in the statistics as well :
               TODO: should not be solved on this level.
            *)
            let () = constants.on_witness source i in
            Fsm.return (Stable_master current_state)
          | Accept(n',i',_v) when i' > new_i || (i' = new_i && n' > n) ->
            (*
               somehow the others decided upon a master and I got no lease expired event.
               or I was running for master and another managed to prepare with a higher n.
               Let's see what's going on, and maybe go back to elections
            *)
            begin
              let run_elections = fst (Slave.time_for_elections constants) in
              if not run_elections
              then
                begin
                  Logger.debug_f_ "%s: stable_master: drop %S (it's still me)" me (string_of msg) >>= fun () ->
                  Fsm.return (Stable_master current_state)
                end
              else
                begin
                  (* Become slave, goto catchup *)
                  Logger.debug_f_ "%s: stable_master: received Accept from new master %S" me (string_of msg) >>= fun () ->
                  let cu_pred = S.get_catchup_start_i constants.store in
                  let new_state = (source,cu_pred,n',i') in
                  Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
                  Fsm.return (Slave_discovered_other_master new_state)
                end
            end
          | Nak _ | Accept _ | Promise _ ->
            begin
              let log_e = ELog (fun () ->
                  Printf.sprintf "stable_master received %S: dropping" (string_of msg))
              in
              Fsm.return ~sides:[log_e] (Stable_master current_state)
            end
      end
    | ElectionTimeout (n', i') ->
      begin
        let log_e = ELog (fun () ->
            Printf.sprintf "ignoring election timeout (%s,%s)" (Sn.string_of n') (Sn.string_of i') )
        in
        Fsm.return ~sides:[log_e] (Stable_master current_state)
      end
    | Quiesce (_, sleep,awake) ->
      begin
        fail_quiesce_request sleep awake Quiesce.Result.FailMaster >>= fun () ->
        Fsm.return (Stable_master current_state)
      end

    | Unquiesce -> Lwt.fail (Failure "Unexpected unquiesce request while running as")

    | DropMaster (sleep, awake) ->
      let state' = (n,new_i, (sleep, awake) :: lease_expire_waiters) in
      Fsm.return (Stable_master state')

(* a master informes the others of a new value by means of Accept
   messages and then waits for Accepted responses *)

let master_dictate constants ms () =
  let {v;n;i; _} = ms in
  let accept_e = EAccept (v,n,i) in
  let mcast_e = EMCast (Accept(n,i,v)) in
  let me = constants.me in
  let others = constants.others in
  let needed = constants.quorum_function (List.length others + 1) in
  let needed' = needed - 1 in
  let ballot = (needed' , [me] ) in

  let log_e =
    ELog (fun () ->
        Printf.sprintf "master_dictate n:%s i:%s needed:%d"
          (Sn.string_of n) (Sn.string_of i) needed'
      )
  in
  let sides =
      [log_e;
       mcast_e;
       accept_e;
      ] in
  start_election_timeout ~from_master:true constants n i >>= fun () ->
  Fsm.return ~sides (Accepteds_check_done (ms, ballot))
