(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE>
*)

open Multi_paxos_type
open Multi_paxos
open Lwt
open Mp_msg.MPMessage


(* a forced slave or someone who is outbidded sends a fake prepare
   in order to receive detailed info from the others in a Nak msg *)
let fake_prepare constants (current_i, current_n) () =
  (* fake a prepare, and hopefully wake up a dormant master *)
  Logger.ign_debug_f_ "Learner.fake_prepare: sending Prepare(-1, %Li)" current_i;
  let fake = Prepare (Sn.of_int (-1), current_i) in
  let sides = [EMCast fake] in
  Fsm.return ~sides (Learner_steady_state (current_n, current_i, None))

let handle_timeout ((n,i,maybe_previous) as state) constants invalidate_lease_start_until n' i' =
  Logger.ign_debug_f_ "steady_state: ignoring lease expiration because I am a learner";
  Fsm.return (Learner_steady_state state)

let handle_from_node ((n,i,previous) as state) (type s) (module S : Store.STORE with type t = s)
      {store; me;on_witness} msg source =
  Logger.ign_debug_f_ "Learner.steady_state n:%s i:%s v:%s store_i:%Li got %S from %s"
    (Sn.string_of n) (Sn.string_of i) (To_string.option Value.value2s previous)
    (match S.consensus_i store with None -> -1L | Some v -> v)
    (string_of msg) source;

  let accept_value ?consensus n' i' v =
    let tail = match consensus with
        None -> []
      | Some (n, i, prev) -> [EConsensus (None, prev, n, i)] in
    let sides = EAccept (v, n', i') :: tail in
    Fsm.return ~sides (Learner_steady_state (n', Sn.succ i', Some v))
  in
  let get_store_i () =
    match S.consensus_i store with
    | None -> Sn.pred Sn.start
    | Some store_i -> store_i
  in
  match msg with
  | Accept (n',i',v) when (n', i') = (n, i) ->
     begin
       on_witness source i';
       (* we should have either a previous value received for this n
          from same master) in that case store_i == i - 2
          or we should have no previous value and store_i = i - 1 *)
       let store_i = get_store_i () in
       match previous, Sn.sub i store_i with
         None, 1L ->
          Logger.ign_debug_f_ "%s: learner: no previous, so not pushing anything" me;
          accept_value n' i' v
       | Some previous, 2L ->
          Logger.ign_debug_f_ "%s: learner: have previous, so that implies consensus (%s, n:%Li, i:%Li)" me (Value.value2s previous) n (Sn.pred i);
        accept_value ~consensus:(n, Sn.pred i, previous) n' i' v
       | None, _ ->
          paxos_fatal me "learner: no previous, mismatch store_i = %Li, i = %Li" store_i i
       | Some _, _ ->
          paxos_fatal me "learner: with previous, mismatch store_i = %Li, i = %Li" store_i i
     end

  | Accept (n',i',v) when (Sn.succ i' = i) && (n' >= n) ->
     if i' = get_store_i () then
       begin
         Logger.ign_debug_f_ "%s: learner, received old accepted (latest applied to store), ignoring" me;
         Fsm.return (Learner_steady_state (n', i, previous))
       end
     else
       accept_value n' i' v

  | Accept (n',i',_) when i' > i || (i' = i && n' > n) ->
     Logger.ign_debug_f_ "Learner.steady_state foreign (%s,%s) from %s <> local (%s,%s) discovered other master"
         (Sn.string_of n') (Sn.string_of i') source (Sn.string_of  n) (Sn.string_of  i);
     Fsm.return (Learner_discovered_other_master (source, n',i'))

  | Accept (_,_,_) ->
     Logger.ign_debug_f_ "Learner.steady_state received old %S for my i, ignoring"
       (string_of msg);
     Fsm.return (Learner_steady_state state)

  | Nak (_,(n2, i2)) when i2 > i ->
     Logger.ign_debug_f_ "%s: got %s => go to catchup" me (string_of msg);
     Fsm.return (Learner_discovered_other_master (source, n2, i2))

  | Nak _
  | Promise _
  | Prepare _
  | Accepted _ ->
     Logger.ign_debug_f_ "Learner.steady state :: ignoring %S" (string_of msg);
     Fsm.return (Learner_steady_state state)

let steady_state (type s) constants state event =
  Slave.common_steady_state
    handle_from_node handle_timeout (fun x -> Learner_steady_state x)
    constants state event

(* a pending slave that discovered another master has to do
   catchup and then go to steady state *)
let discovered_other_master (type s) constants state () =
  let (master, future_n, future_i) = state in
  let me = constants.me
  and other_cfgs = constants.other_cfgs
  and store = constants.store
  and tlog_coll = constants.tlog_coll
  in
  let module S = (val constants.store_module : Store.STORE with type t = s) in

  Logger.ign_debug_f_ "%s: Learner.discovered_other_master: catching up from %s @ %s"
                           me master (Sn.string_of future_i);
  let cluster_id = constants.cluster_id in
  let tls_ctx = constants.catchup_tls_ctx in
  Catchup.catchup
    ~tls_ctx
    ~tcp_keepalive:constants.tcp_keepalive
    ~stop:constants.stop
    me other_cfgs ~cluster_id ((module S), store, tlog_coll) master >>= fun () ->

  let i = S.get_succ_store_i store in
  Fsm.return (Learner_fake_prepare (i, future_n))
