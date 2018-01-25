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




type finished_fun = Store.update_result -> unit Lwt.t
type master_option = (finished_fun list) option

type v_limits = int * (Value.t * int) list
(* number of times None was chosen;
   all Some v promises and their frequency *)
type n = Sn.t
type i = Sn.t
type slave_awaiters = (unit Lwt.t * unit Lwt.u) list

type mballot = int * Messaging.id list
type master_state = { mo:master_option;
                      v: Value.t ;
                      n:n;
                      i:i;
                      lew: slave_awaiters}

type transitions =
  (* dummy to always have a previous transition *)
  | Start_transition

  (* election only *)
  | Election_suggest of (n * int)

  (* slave or pending slave *)
  | Slave_fake_prepare of (n * i)
  | Slave_steady_state of (n * i * Value.t option) (* value received for this n and previous i *)
  | Slave_discovered_other_master of (Messaging.id * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n )

  | Promises_check_done of (n * i *
                              Messaging.id list *
                              v_limits *
                              slave_awaiters * int)
  | Wait_for_promises of (n * i * Messaging.id list *
                            v_limits *
                            slave_awaiters * int)
  | Accepteds_check_done of (master_state * mballot)
  | Wait_for_accepteds   of (master_state * mballot)

  (* active master only *)
  | Master_consensus of master_state
  | Stable_master    of (n * i * slave_awaiters)
  | Master_dictate   of master_state


type learner_transitions =
  | Learner_fake_prepare of (n * i)
  | Learner_steady_state of (n * i * Value.t option) (* value received for this n and previous i *)
  | Learner_discovered_other_master of (Messaging.id *
                                        Mp_msg.MPMessage.n * Mp_msg.MPMessage.n )

(* utility functions *)
let show_transition = function
  | Start_transition -> "Start_transition"
  | Election_suggest _ -> "Election_suggest"
  | Slave_fake_prepare _ -> "Slave_fake_prepare"
  | Slave_steady_state _ -> "Slave_steady_state"
  | Slave_discovered_other_master _ -> "Slave_discovered_other_master"
  | Wait_for_promises _ -> "Wait_for_promises"
  | Promises_check_done _ -> "Promises_check_done"
  | Wait_for_accepteds _ -> "Wait_for_accepteds"
  | Accepteds_check_done _ -> "Accepteds_check_done"
  | Master_consensus _ -> "Master_consensus"
  | Stable_master _ -> "Stable_master"
  | Master_dictate _ -> "Master_dictate"

type effect =
  | ELog of (unit -> string)
  | EMCast  of Mp_msg.MPMessage.t
  | ESend of Mp_msg.MPMessage.t * Messaging.id
  | EAccept of (Value.t * n * i)
  | EStartElectionTimeout of n * i
  | EConsensus of (master_option * Value.t * n * i)
  | EGen of (unit -> unit Lwt.t)
