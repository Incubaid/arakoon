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

open Lwt
open Mp_msg
open MPMessage
open Lwt_buffer
open Fsm
open Multi_paxos_type
open Multi_paxos
open Master_type


(* in case of election, everybody suggests himself *)
let election_suggest (type s) constants (n, counter) () =
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  let i = S.get_succ_store_i constants.store in
  let vo = constants.get_value i in
  let me = constants.me in
  let v_lims, msg =
    match vo with
      | None ->
        (1,[]) , "None"
      | Some x -> (0,[(x,1)]) , "Some _"
  in
  Logger.info_f_ "%s: election_suggest: n=%s i=%s %s" me  (Sn.string_of n) (Sn.string_of i) msg >>= fun () ->
  start_election_timeout constants n i >>= fun () ->
  let delay =
    match constants.master with
      | Preferred ps when not (List.mem me ps) -> 1 + (constants.lease_expiration /2) - counter
      | Elected | ReadOnly |Forced _ |Preferred _ -> 0
  in
  let df = float delay in
  Lwt.ignore_result
    (Lwt_unix.sleep df >>= fun () -> mcast constants (Prepare (n,i)));
  let who_voted = [me] in
  let state = (n, i, who_voted, v_lims, [], counter) in
  Fsm.return (Promises_check_done state)

let read_only constants (_state : unit) () =
  Lwt_unix.sleep 60.0 >>= fun () ->
  Logger.debug_f_ "%s: read_only ..." constants.me >>= fun () ->
  Fsm.return Read_only


(* a potential master is waiting for promises and if enough
   promises are received the value is accepted and Accept is broadcasted *)
let promises_check_done constants state () =
  let n, i, who_voted, (v_lims:v_limits), lease_expire_waiters, counter = state in
  (*
     3 cases:
     -) too early to decide
     -) consensus on some value (either my wanted or another)
     -) no consensus possible anymore. (split vote)
  *)
  let me = constants.me in
  let nnones, v_s = v_lims in
  let bv,bf =
    begin
      match v_s with
        | [] ->  Value.create_master_value constants.tlog_coll i me, 0
        | hd::_ -> hd
    end in
  let nnodes = List.length constants.others + 1 in
  let needed = constants.quorum_function nnodes in
  let nvoted = List.length who_voted in
  if bf + nnones = needed
  then
    begin
      Logger.info_f_ "%s: promises_check_done: consensus on %s" me (Sn.string_of i)
      >>= fun () ->
      push_value constants bv n i >>= fun () ->
      let new_ballot = (needed-1 , [me] ) in
      let ms = {mo = None;v = bv;n;i;lew = lease_expire_waiters} in
      Fsm.return (Accepteds_check_done (ms, new_ballot))
    end
  else (* bf < needed *)
  if nvoted < nnodes
  then Fsm.return (Wait_for_promises state)
  else (* split vote *)
    begin
      if am_forced_master constants me
      then
        begin
          Logger.warning_f_ "%s: promises_check_done: split vote, going to forced_master_suggest" me >>= fun () ->
          Fsm.return (Election_suggest (n, 0))
        end
      else
      if is_election constants
      then
        begin
          Logger.warning_f_ "%s: promises_check_done: split vote, going back to elections" me >>= fun () ->
          let n' = update_n constants n in
          Fsm.return (Election_suggest (n', counter + 1))
        end
      else
        paxos_fatal me "slave checking for promises"
    end


(* a potential master is waiting for promises and receives a msg *)
let wait_for_promises (type s) constants state event =
  let me = constants.me in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  let (n, i, who_voted, v_lims, lease_expire_waiters, counter) = state in
  match event with
    | FromNode (msg,source) ->
      begin
        let who_voted_s = Log_extra.list2s (fun s -> s) who_voted in
        let log_e0 =
          ELog(fun () ->
              Printf.sprintf "wait_for_promises:n=%s i=%s who_voted = %s received %s from %s"
                (Sn.string_of n) (Sn.string_of i) who_voted_s
                (string_of msg) source
            )
        in
        let drop msg reason =
          let log_e = ELog(fun () ->
              Printf.sprintf "dropping %s because: %s" (string_of msg) reason)
          in
          Fsm.return ~sides:[log_e0;log_e] (Wait_for_promises state)
        in
        begin
          match msg with
            | Promise (n' ,_i', _limit) when n' < n ->
              let reason = Printf.sprintf "old promise (%s < %s)" (Sn.string_of n') (Sn.string_of n) in
              drop msg reason
            | Promise (n',i', _limit) when n' = n && i' < i ->
              let reason = Printf.sprintf "old promise with lower i: %s < %s"
                             (Sn.string_of i') (Sn.string_of i)
              in
              drop msg reason
            | Promise (n' ,new_i, limit) when n' = n ->
              if List.mem source who_voted then
                drop msg "duplicate promise"
              else
                let v_lims' =
                  begin
                    if new_i < i then
                      let (nnones, nsomes) = v_lims in
                      (nnones+1, nsomes)
                    else
                      update_votes v_lims limit
                  end in
                let who_voted' = source :: who_voted in
                let state' = (n, i, who_voted', v_lims', lease_expire_waiters, counter) in
                Fsm.return (Promises_check_done state')
            | Promise (n' ,_i', _limit) -> (* n ' > n *)
              begin
                Logger.debug_f_ "%s: Received Promise from previous incarnation. Bumping n from %s over %s." me
                  (Sn.string_of n) (Sn.string_of n')
                >>= fun () ->
                let new_n = update_n constants n' in
                Fsm.return (Election_suggest (new_n, counter + 1))
              end
            | Nak (n',(_n'',_i')) when n' < n ->
              begin
                Logger.debug_f_ "%s: wait_for_promises:: received old %S (ignoring)" me (string_of msg)
                >>= fun () ->
                Fsm.return (Wait_for_promises state)
              end
            | Nak (n',(_n'',_i')) when n' > n ->
              begin
                Logger.debug_f_ "%s: Received Nak from previous incarnation. Bumping n from %s over %s." me
                  (Sn.string_of n) (Sn.string_of n')
                >>= fun () ->
                let new_n = update_n constants n' in
                Fsm.return (Election_suggest (new_n, counter + 1))
              end
            | Nak (_n',(n'',i')) -> (* n' = n *)
              begin
                Logger.debug_f_ "%s: wait_for_promises:: received %S for my Prep(i=%s, _) " me
                  (string_of msg)
                  (Sn.string_of i)
                >>= fun () ->
                if am_forced_master constants me
                then
                  begin
                    Logger.debug_f_ "%s: wait_for_promises; forcing new master suggest" me >>= fun () ->
                    let n3 = update_n constants (max n n'') in
                    Fsm.return (Election_suggest (n3,0))
                  end
                else
                  (* if is_election constants
                     then *)
                  begin
                    Logger.debug_f_ "%s: wait_for_promises; discovered other node" me
                    >>= fun () ->
                    if i' > i
                    then
                      let cu_pred = S.get_catchup_start_i constants.store in
                      Fsm.return (Slave_discovered_other_master (source,cu_pred,n'',i'))
                    else
                      let new_n = update_n constants (max n n'') in
                      Fsm.return (Election_suggest (new_n, counter + 1))
                  end
              end
            | Prepare (n',i') ->
              begin
                if (am_forced_master constants me) && n' > 0L
                then
                  begin
                    Logger.debug_f_ "%s: wait_for_promises:dueling; forcing new master suggest" me >>= fun () ->
                    let reply = Nak (n', (n,i))  in
                    constants.send reply me source >>= fun () ->
                    let new_n = update_n constants n' in
                    Fsm.return (Election_suggest (new_n, 0))
                  end
                else

                  handle_prepare constants source n n' i' >>= function
                  | Nak_sent ->
                    Logger.debug_f_ "%s: wait_for_promises: resending prepare" me >>= fun () ->
                    let reply = Prepare(n, i) in
                    constants.send reply me source >>= fun () ->
                    Fsm.return (Wait_for_promises state)
                  | Prepare_dropped ->
                    Fsm.return (Wait_for_promises state)
                  | Promise_sent_up2date ->
                    start_lease_expiration_thread constants >>= fun () ->
                    Fsm.return (Slave_steady_state (n', i, None))
                  | Promise_sent_needs_catchup ->
                    let i = S.get_catchup_start_i constants.store in
                    Fsm.return (Slave_discovered_other_master (source, i, n', i'))
              end
            | Accept (n',_i,_v) when n' < n ->
              begin
                if i < _i
                then
                  begin
                    Logger.debug_f_ "%s: wait_for_promises: still have an active master (received %s) -> catching up from master" me  (string_of msg) >>= fun () ->
                    Fsm.return (Slave_discovered_other_master (source, i, n', _i))
                  end
                else
                  Logger.debug_f_ "%s: wait_for_promises: ignoring old Accept %s" me (Sn.string_of n')
                  >>= fun () ->
                  Fsm.return (Wait_for_promises state)
              end
            | Accept (n',_i,_v) ->
              if n' = n && _i < i
              then
                begin
                  Logger.debug_f_ "%s: wait_for_promises: ignoring %s (my i is %s)" me (string_of msg) (Sn.string_of i) >>= fun () ->
                  Fsm.return (Wait_for_promises state)
                end
              else
                begin
                  Logger.debug_f_ "%s: wait_for_promises: received %S -> back to fake prepare" me  (string_of msg) >>= fun () ->
                  Fsm.return (Slave_fake_prepare (i,n))
                end
            | Accepted (n',_i) when n' < n ->
              begin
                Logger.debug_f_ "%s: wait_for_promises: ignoring old Accepted %s" me (Sn.string_of n') >>= fun () ->
                Fsm.return (Wait_for_promises state)
              end
            | Accepted (n',_i) -> (* n' >= n *)
              begin
                Logger.debug_f_ "%s: Received Nak from previous incarnation. Bumping n from %s over %s." me (Sn.string_of n) (Sn.string_of n')
                >>= fun () ->
                let new_n = update_n constants n' in
                Fsm.return (Election_suggest (new_n, counter + 1))
              end
        end
      end
    | ElectionTimeout (n', i') ->
      let (n,i,_who_voted, _v_lims, _lease_expire_waiters, counter) = state in
      if n' = n && i' = i && not ( S.quiesced constants.store )
      then
        begin
          Logger.debug_f_ "%s: wait_for_promises: election timeout, restart from scratch" me
          >>= fun () ->
          Fsm.return (Election_suggest (n, counter + 1))
        end
      else
        begin
          Fsm.return (Wait_for_promises state)
        end
    | LeaseExpired _ ->
      Logger.debug_f_ "%s: Ignoring lease expiration" me >>= fun () ->
      Fsm.return (Wait_for_promises state)
    | FromClient _ ->
      paxos_fatal me "wait_for_promises: don't want FromClient"

    | Quiesce (mode, sleep,awake) ->
        handle_quiesce_request (module S) constants.store mode sleep awake >>= fun () ->
      Fsm.return (Wait_for_promises state)

    | Unquiesce ->
      handle_unquiesce_request constants >>= fun () ->
      Fsm.return (Wait_for_promises state)
    | DropMaster (sleep, awake) ->
      Multi_paxos.safe_wakeup sleep awake () >>= fun () ->
      Fsm.return (Wait_for_promises state)



(* a (potential or full) master is waiting for accepteds and if he has received
   enough, consensus is reached and he becomes a full master *)
let lost_master_role = function
  | None -> Lwt.return ()
  | Some finished_funs ->
    begin
      let msg = "lost master role during wait_for_accepteds while handling client request" in
      let rc = Arakoon_exc.E_NO_LONGER_MASTER in
      let result = Store.Update_fail (rc, msg) in
      Lwt_list.iter_s (fun f -> f result) finished_funs
    end

let accepteds_check_done _constants ((ms,ballot)as state) () =
  let needed = fst ballot in
  if needed = 0
  then
    begin
      let log_e =
        ELog (fun () ->
          let n = ms.n and i = ms.i in
          Printf.sprintf "accepted_check_done :: we're done! returning %s %s"
            (Sn.string_of n) ( Sn.string_of i )
          )
      in
      let sides = [log_e] in
      Fsm.return ~sides (Master_consensus ms)
    end
  else
    Fsm.return (Wait_for_accepteds state)


(* a (potential or full) master is waiting for accepteds and receives a msg *)
let wait_for_accepteds
      (type s) constants ((ms,ballot)as state)
      (event:paxos_event) =
  let me = constants.me in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  let {mo;n;v;i;lew} = ms in
  match event with
    | FromNode(msg,source) ->
      begin
        (* TODO: what happens with the client request
           when I fall back to a state without mo ? *)
        let drop msg reason =
          let log_e =
            ELog
              (fun () ->
               Printf.sprintf "dropping %s because : '%s'"
                              (MPMessage.string_of msg) reason)
          in
          Fsm.return ~sides:[log_e] (Wait_for_accepteds state)
        in
        let needed, already_voted = ballot in
        Logger.debug_f_ "%s: wait_for_accepteds(%i to go) got %S from %s" me needed
          (MPMessage.string_of msg) source >>= fun () ->
        begin
          match msg with
            | Accepted (n',i') when (n',i')=(n,i)  ->
              begin
                let () = constants.on_witness source i' in
                if List.mem source already_voted then
                  let reason = Printf.sprintf "%s already voted" source in
                  drop msg reason
                else
                  let ballot' = needed -1, source :: already_voted in
                  Fsm.return (Accepteds_check_done (ms, ballot'))
              end
            | Accepted (n',i') when n' = n && i' < i ->
              begin
                let () = constants.on_witness source i' in
                Logger.debug_f_ "%s: wait_for_accepteds: received older Accepted for my n: ignoring" me
                >>= fun () ->
                Fsm.return (Wait_for_accepteds state)
              end
            | Accepted (n',i') when n' < n ->
              begin
                let () = constants.on_witness source i' in
                let reason = Printf.sprintf "dropping old %S we're @ (%s,%s)" (string_of msg)
                               (Sn.string_of n) (Sn.string_of i) in
                drop msg reason
              end
            | Accepted (n',_i') -> (* n' > n *)
              paxos_fatal me "wait_for_accepteds:: received %S with n'=%s > my n=%s FATAL" (string_of msg) (Sn.string_of n') (Sn.string_of n)

            | Promise(n',i', _limit) ->
              begin
                let () = constants.on_witness source i' in
                if n' <= n then
                  begin
                    let reason = Printf.sprintf
                                   "already reached consensus on (%s,%s)"
                                   (Sn.string_of n) (Sn.string_of i)
                    in
                    drop msg reason
                  end
                else
                  begin
                    let reason = Printf.sprintf "future Promise(%s,%s), local (%s,%s)"
                                   (Sn.string_of n') (Sn.string_of i') (Sn.string_of n) (Sn.string_of i) in
                    drop msg reason
                  end
              end
            | Prepare (n',i') -> (* n' > n *)
              begin
                if am_forced_master constants me
                then
                  if n' <= n
                  then
                    let reply = Nak( n', (n,i) ) in
                    constants.send reply me source >>= fun () ->
                    let followup = Accept( n, i, v) in
                    constants.send followup me source >>= fun () ->
                    Fsm.return (Wait_for_accepteds state)
                  else
                    begin
                      lost_master_role mo >>= fun () ->
                      Multi_paxos.safe_wakeup_all () lew >>= fun () ->
                      Fsm.return (Election_suggest (n', 0))
                    end
                else
                  begin
                    handle_prepare constants source n n' i' >>=
                    function
                    | Prepare_dropped
                    | Nak_sent ->
                      Fsm.return( Wait_for_accepteds state)
                    | Promise_sent_up2date ->
                      begin
                        lost_master_role mo >>= fun () ->
                        Multi_paxos.safe_wakeup_all () lew >>= fun () ->
                        start_lease_expiration_thread constants >>= fun () ->
                        Fsm.return (Slave_steady_state (n', i, None))
                      end
                    | Promise_sent_needs_catchup ->
                      begin
                        let i = S.get_catchup_start_i constants.store in
                        lost_master_role mo >>= fun () ->
                        Multi_paxos.safe_wakeup_all () lew >>= fun () ->
                        Fsm.return (Slave_discovered_other_master (source, i, n', i'))
                      end
                  end
              end
            | Nak (_n',_i) ->
              begin
                Logger.debug_f_ "%s: wait_for_accepted: ignoring %S from %s when collecting accepteds" me
                  (MPMessage.string_of msg) source >>= fun () ->
                Fsm.return (Wait_for_accepteds state)
              end
            | Accept (n',i',v') when (n',i',v')=(n,i,v) ->
              begin
                Logger.debug_f_ "%s: wait_for_accepted: ignoring extra Accept %S" me (string_of msg) >>= fun () ->
                Fsm.return (Wait_for_accepteds state)
              end
            | Accept (n',i',_v') when i' > i || (i' = i && n' > n) ->
              (* check lease, if we're inside, drop (how could this have happened?)
                 otherwise, we've lost master role
              *)
              let run_elections, _why = Slave.time_for_elections constants in
              if not run_elections
              then
                begin
                  Logger.debug_f_ "%s: wait_for_accepteds: drop %S (it's still me)" me (string_of msg) >>= fun () ->
                  Fsm.return (Wait_for_accepteds state)
                end
              else
                begin
                  lost_master_role mo >>= fun () ->
                  Multi_paxos.safe_wakeup_all () lew >>= fun () ->
                  begin
                    (* Become slave, goto catchup *)
                    Logger.debug_f_ "%s: wait_for_accepteds: received Accept from new master %S" me (string_of msg) >>= fun () ->
                    let cu_pred = S.get_catchup_start_i constants.store in
                    let new_state = (source,cu_pred,n',i') in
                    Logger.debug_f_ "%s: wait_for_accepteds: drop %S (it's still me)" me (string_of msg) >>= fun () ->
                    Fsm.return (Slave_discovered_other_master new_state)
                  end
                end
            | Accept (n',i',_v) when i' < i || (i' = i && n' < n)  ->
               begin
                 Logger.debug_f_ "%s: wait_for_accepted: dropping old Accept %S" me (string_of msg) >>= fun () ->
                 Fsm.return (Wait_for_accepteds state)
               end
            | Accept (n',i',_v') ->
                paxos_fatal me "Unable to handle unexpected message %S, %s, %s" (string_of msg) (Sn.string_of i') (Sn.string_of n')
        end
      end
    | FromClient _       -> paxos_fatal me "no FromClient should get here"
    | LeaseExpired _     -> paxos_fatal me "no LeaseExpired should get here"
    | ElectionTimeout (n', i') ->
      begin
        let here = "wait_for_accepteds : election timeout " in
        if n' <> n || i' <> i then
          begin
            let log_e =
              ELog (fun () ->
                  Printf.sprintf
                    "%s ignoring old timeout %s<>%s || %s<>%s"
                    here
                    (Sn.string_of n') (Sn.string_of n)
                    (Sn.string_of i') (Sn.string_of i)
                )
            in
            Fsm.return ~sides:[log_e] (Wait_for_accepteds state)
          end
        else
          begin
            Logger.debug_f_ "%s: going to RESEND Accept messages" me >>= fun () ->
            let _needed, already_voted = ballot in
            let msg = Accept(n,i,v) in
            let silent_others = List.filter (fun o -> not (List.mem o already_voted))
                                  constants.others in
            Lwt_list.iter_s (fun o -> constants.send msg me o) silent_others >>= fun () ->
            mcast constants msg >>= fun () ->
            start_election_timeout constants n i >>= fun () ->
            Fsm.return (Wait_for_accepteds state)
          end
      end
    | Quiesce (_mode, sleep,awake) ->
      fail_quiesce_request sleep awake Quiesce.Result.FailMaster >>= fun () ->
       Fsm.return (Wait_for_accepteds state)

    | Unquiesce ->
       Lwt.fail (Failure "Unexpected unquiesce request while running as master")

    | DropMaster (sleep, awake) ->
       let lew' = (sleep,awake) :: ms.lew in
       let ms' = {ms with lew = lew'} in
       Fsm.return (Wait_for_accepteds (ms', ballot))


(* the state machine translator *)

type wanted_event =
  | Node
  | Inject
  | Election_timeout
  | Client

let product_wanted_to_string pws =
  Log_extra.list2s
    (function
      | Node -> "Node"
      | Inject -> "Inject"
      | Election_timeout -> "Election_timeout"
      | Client -> "Client")
    pws

let machine constants =
  let nop = [] in
  let full = [Node; Inject; Election_timeout; Client] in
  let node_and_timeout = [Node; Election_timeout] in
  let node_and_inject_and_timeout = [Node; Inject; Election_timeout] in
  function
    | Slave_fake_prepare i ->
      (Unit_arg (Slave.slave_fake_prepare constants i), nop)
    | Slave_steady_state state ->
      (Msg_arg (Slave.slave_steady_state constants state), full)
    | Slave_discovered_other_master state ->
      (Unit_arg (Slave.slave_discovered_other_master constants state), nop)

    | Wait_for_promises state ->
      (Msg_arg (wait_for_promises constants state), node_and_inject_and_timeout)
    | Promises_check_done state ->
      (Unit_arg (promises_check_done constants state), nop)
    | Wait_for_accepteds state ->
      (Msg_arg (wait_for_accepteds constants state), node_and_timeout)
    | Accepteds_check_done state ->
      (Unit_arg (accepteds_check_done constants state), nop)

    | Master_consensus ms ->
      (Unit_arg (Master.master_consensus constants ms), nop)
    | Stable_master state ->
      (Msg_arg (Master.stable_master constants state), full)
    | Master_dictate ms ->
      (Unit_arg (Master.master_dictate constants ms), nop)

    | Election_suggest state ->
      (Unit_arg (election_suggest constants state), nop)
    | Read_only ->
      (Unit_arg (read_only constants ()), nop)
    | Start_transition -> failwith "Start_transition?"

(* Warning: This currently means we have only 1 fsm / executable. *)
let __state = ref Start_transition

let trace_transition key =
  __state := key;
  Lwt.return ()
and pull_state () = (show_transition !__state)


let prio = function
  | Inject -> 0
  | Election_timeout -> 1
  | Node   -> 2
  | Client -> 3


type ('a,'b,'c) buffers =
  {client_buffer : 'a Lwt_buffer.t;
   node_buffer   : 'b Lwt_buffer.t;
   inject_buffer : 'c Lwt_buffer.t;
   election_timeout_buffer: 'c Lwt_buffer.t;
  }
let make_buffers (a,b,c,d) = {
  client_buffer = a;
  node_buffer = b;
  inject_buffer = c;
  election_timeout_buffer = d;
}

let paxos_produce buffers constants product_wanted =
  let me = constants.me in
  let wmsg = product_wanted_to_string product_wanted in
  let () = match product_wanted with
    | [] -> failwith "No products wanted should not happen here"
    | _ -> () in
  Lwt.catch
    (fun () ->
       Logger.debug_f_ "%s: T:waiting for event (%s)" me wmsg >>= fun () ->
       let t0 = Unix.gettimeofday () in

       let wait_for_buffer = function
         | Client -> Lwt_buffer.wait_for_item buffers.client_buffer
         | Node -> Lwt_buffer.wait_for_item buffers.node_buffer
         | Inject -> Lwt_buffer.wait_for_item buffers.inject_buffer
         | Election_timeout -> Lwt_buffer.wait_for_item buffers.election_timeout_buffer in
       let waiters = List.map
                       wait_for_buffer
                       product_wanted in
       Lwt.pick waiters >>= fun () ->

       let t1 = Unix.gettimeofday () in
       let d = t1 -. t0 in
       Logger.debug_f_ "%s: T:waiting for event took:%f" me d >>= fun () ->

       let get_highest_prio_evt r_list =
         let f acc b = match acc with
           | None -> Some b
           | Some pb ->
             if prio(b) > prio (pb)
             then acc
             else Some b
         in
         let best = List.fold_left f None r_list in
         match best with
           | Some Inject ->
             begin
               Logger.debug_f_ "%s: taking from inject" me >>= fun () ->
               Lwt_buffer.take buffers.inject_buffer
             end
           | Some Client ->
             begin
               Lwt_buffer.harvest buffers.client_buffer >>= fun reqs ->
               let event = FromClient reqs in
               Lwt.return event
             end
           | Some Node ->
             begin
               Lwt_buffer.take buffers.node_buffer >>= fun (msg,source) ->
               let msg2 = MPMessage.of_generic msg in
               let t = Unix.gettimeofday () in
               Logger.debug_f_ "%s: %f SEQ: %s => %s : %s" me t source me (Mp_msg.MPMessage.string_of msg2)
               >>= fun () ->
               Lwt.return (FromNode (msg2,source))
             end
           | Some Election_timeout ->
             begin
               Logger.debug_f_ "%s: taking from timeout" me >>= fun () ->
               Lwt_buffer.take buffers.election_timeout_buffer
             end
           | None ->
             Lwt.fail ( Failure "FSM BAILED: No events ready while there should be" )
       in
       let buffer_has_item = function
         | Client -> Lwt_buffer.has_item buffers.client_buffer
         | Node -> Lwt_buffer.has_item buffers.node_buffer
         | Inject -> Lwt_buffer.has_item buffers.inject_buffer
         | Election_timeout -> Lwt_buffer.has_item buffers.election_timeout_buffer in
       let ready_list =
         List.filter
           buffer_has_item
           product_wanted in
       get_highest_prio_evt ready_list
    )
    (fun e -> Logger.debug_f_ "%s: ZYX %s" me (Printexc.to_string e) >>= fun () -> Lwt.fail e)




let _execute_effects constants e =
  match e with
    | ELog build          -> Logger.debug_f_ "%s: %s" constants.me ("PURE: " ^ build ())
    | EMCast msg          -> mcast constants msg
    | EAccept (v,n,i)     -> constants.on_accept (v,n,i)
    | ESend (msg, target) -> constants.send msg constants.me target
    | EStartElectionTimeout (n, i) -> start_election_timeout constants n i

    | EConsensus (ofinished_funs, v,n,i) ->
      begin
        constants.on_consensus (v,n,i) >>= fun (urs: Store.update_result list) ->
        begin
          match ofinished_funs with
            | None ->
              Lwt.return_unit
            | Some finished_funs ->
              let rec loop ffs urs =
                match (ffs,urs) with
                  | [],[] -> Lwt.return ()
                  | finished_f :: ffs , update_result :: urs ->
                    finished_f update_result >>= fun () ->
                    loop ffs urs
                  | _,_ -> failwith "mismatch"
              in
              loop finished_funs urs
        end >>= fun () ->
        if Value.is_master_set v
        then
          start_lease_expiration_thread constants
        else
          Lwt.return ()
      end
    | EGen f -> f ()


(* the entry methods *)

let enter_forced_slave ?(stop = ref false) constants buffers new_i =
  let me = constants.me in
  Logger.debug_f_ "%s: +starting FSM for forced_slave." me >>= fun () ->
  let trace = trace_transition in
  let produce = paxos_produce buffers constants in
  let new_n = update_n constants Sn.start in

  Lwt.catch
    (fun () ->
       Fsm.loop ~trace ~stop
         (_execute_effects constants)
         produce
         (machine constants)
         (Slave.slave_fake_prepare constants (new_i,new_n))
    )
    (fun exn ->
       Logger.warning_ ~exn "FSM BAILED due to uncaught exception"
       >>= fun () -> Lwt.fail exn
    )

let enter_forced_master ?(stop = ref false) constants buffers _current_i =
  let me = constants.me in
  Logger.debug_f_ "%s: +starting FSM for forced_master." me >>= fun () ->
  let current_n = update_n constants Sn.start in
  let trace = trace_transition in
  let produce = paxos_produce buffers constants in
  Lwt.catch
    (fun () ->
       Fsm.loop ~trace ~stop
         (_execute_effects constants)
         produce
         (machine constants)
         (election_suggest constants (current_n, 0))
    )
    (fun e ->
       Logger.debug_f_ "%s: FSM BAILED due to uncaught exception %s" me (Printexc.to_string e)
       >>= fun () -> Lwt.fail e
    )

let enter_simple_paxos (type s) ?(stop = ref false) constants buffers current_i =
  let me = constants.me in
  let current_n = update_n constants Sn.start in
  let trace = trace_transition in
  let produce = paxos_produce buffers constants in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  let other_master = match S.who_master constants.store with
    | None -> false
    | Some (_, ls) ->
      let diff = (Unix.gettimeofday ()) -. ls in
      diff < float constants.lease_expiration in
  let run start_state =
    Lwt.catch
      (fun () ->
         Fsm.loop ~trace ~stop
           (_execute_effects constants)
           produce
           (machine constants)
           start_state
      )
      (fun e ->
         Logger.debug_f_ "%s: FSM BAILED (run_election) due to uncaught exception %s" me
           (Printexc.to_string e)
         >>= fun () -> Lwt.fail e
      ) in
  if other_master
  then
    begin
      Logger.debug_f_ "%s: +starting slave_fake_prepare." me >>= fun () ->
      start_lease_expiration_thread constants >>= fun () ->
      run (Slave.slave_fake_prepare constants (current_i,current_n))
    end
  else
    begin
      Logger.debug_f_ "%s: +starting FSM election." me >>= fun () ->
      run (election_suggest constants (current_n, 0))
    end

let enter_read_only ?(stop = ref false) constants buffers _current_i =
  let me = constants.me in
  Logger.debug_f_ "%s: +starting FSM for read_only." me >>= fun () ->
  let trace = trace_transition in
  let produce = paxos_produce buffers constants in
  Lwt.catch
    (fun () ->
       Fsm.loop ~trace ~stop
         (_execute_effects constants)
         produce
         (machine constants)
         (read_only constants ())
    )
    (fun exn ->
       Logger.warning_ ~exn "READ ONLY BAILS OUT" >>= fun () ->
       Lwt.fail exn
    )

let expect_run_forced_slave constants buffers expected step_count new_i =
  Logger.debug_f_ "%s: +starting forced_slave FSM with expect" constants.me >>= fun () ->
  let produce = paxos_produce buffers constants in
  Lwt.catch
    (fun () ->
       Fsm.expect_loop
         (_execute_effects constants)
         expected step_count Start_transition produce
         (machine constants) (Slave.slave_fake_prepare constants new_i)
    )
    (fun e ->
       Logger.debug_f_ "%s: FSM BAILED due to uncaught exception %s" constants.me (Printexc.to_string e)
       >>= fun () -> Lwt.fail e
    )

let expect_run_forced_master constants buffers expected step_count current_n _current_i =
  let produce = paxos_produce buffers constants in
  Logger.debug_f_ "%s: +starting forced_master FSM with expect" constants.me >>= fun () ->
  Lwt.catch
    (fun () ->
       Fsm.expect_loop
         (_execute_effects constants)
         expected step_count Start_transition produce
         (machine constants)
         (election_suggest constants (current_n, 0))
    )
    (fun e ->
       Logger.debug_f_ "%s: FSM BAILED due to uncaught exception %s" constants.me (Printexc.to_string e)
       >>= fun () -> Lwt.fail e
    )
