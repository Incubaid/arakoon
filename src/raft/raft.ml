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

open Std
open Compare
open Lwt

type node_id = string
type term = Int64.t
type index = Int64.t
type entry = string (* consensus on opaque strings *)

type follower_state = {
  mutable latest_heartbeat : float;
}

type leader_state = {
  next_index : (node_id, index) Hashtbl.t;
  match_index : (node_id, index) Hashtbl.t;
}

type role =
  | Follower of follower_state
  | Candidate
  | Leader of leader_state

class type transactionLog = object
  method get_entry : index -> (entry * term) option Lwt.t
  method get_max_i : unit -> index option Lwt.t
end

type persisted_state = {
  current_term : term;
  voted_for : node_id option;
  log : transactionLog;
}

type state = {
  persisted_state : persisted_state;
  commit_index : index;
}

type request_vote_request = {
  (* candidate_id : node_id; *)
  last_log_index : index option;
  last_log_term : index option;
}
type request_vote_response = {
  vote_granted : bool;
}

type append_entries_request = {
  (* leader_id : node_id; *)
  prev_log_index : index;
  prev_log_term : index;
  entries : entry list;
  leader_commit : index;
}
type append_entries_response = {
  success : bool;
}

type message_base =
  | AppendEntriesRequest of append_entries_request
  | AppendEntriesResponse of append_entries_response
  | RequestVote of request_vote_request
  | RequestVoteResponse of request_vote_response

type message = term * message_base

type event =
  | NodeMessage of node_id * message
  | HeartbeatTimeout of float (* time of timeout start? *)
  | ElectionTimeout of term (**)


let follower_handle_event state fs = function
  | HeartbeatTimeout t ->
     if fs.latest_heartbeat <= t then
       [`PromoteToCandidate]
     else
       [] (* ignore old heartbeat timeout *)
  | ElectionTimeout _ ->
     (* ignore *)
     []
  | NodeMessage (from, (term, msg)) ->
     begin
       match msg with
       | AppendEntriesRequest ae ->
          begin
            let response success =
              `Send (from, (AppendEntriesResponse { success; })) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [response false]
            | GT ->
               [`UpdateTerm (Some from, term);
                `AppendEntries ae;] (* do other stuff too? *)
            | EQ ->
               (* TODO check that we have prev_log_index ... but that is an effect!! *)
               (* keep floating state .. latest transaction log entry + it's term,
                  so in the happy path we can go quick (and handle it here?) *)
               [`AppendEntries ae;
                (* response true; CANT ANSWER THIS HERE. *)]
          end
       | AppendEntriesResponse _
       | RequestVoteResponse _ ->
          (* ignore *)
          []
       | RequestVote _ ->
          begin
            let vote_response vote_granted =
              `Send (from, (RequestVoteResponse { vote_granted; })) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [vote_response false]
            | GT ->
               (* newer term, so no leader yet ... grant this one *)
               [`UpdateTerm (Some from, term);
                vote_response true]
            | EQ ->
               match state.persisted_state.voted_for with
               | None ->
                  (* no leader chosen yet for this term *)
                  [`UpdateTerm (Some from, term);
                   vote_response true]
               | Some leader when String.(leader = from) ->
                  (* already voted for this node, repeating our response *)
                  [vote_response true]
               | Some _ ->
                  (* already voted for another node, denying *)
                 [vote_response false]
          end
     end

let candidate_handle_event state = function
  | HeartbeatTimeout _ ->
     (* ignore, we're in candidate state already *)
     []
  | ElectionTimeout t ->
     if Int64.(t = state.persisted_state.current_term)
     then
       (* bump term
          send request vote messages
          launch new election timeout
        *)
       []
     else
       (* old timeout, ignoring *)
       []
  | NodeMessage (from, (term, msg)) ->
     begin
       match msg with
       | AppendEntriesRequest ae ->
          begin
            let response success =
              `Send (from, (AppendEntriesResponse { success; })) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [response false]
            | GT ->
               (* TODO
                  go to follower
                  try to append entries
                  process original message in follower state? -> that's not such a bad idea...
                *)
               [`UpdateTerm (Some from, term);
                `ToFollower]
            | EQ ->
               (* TODO
                  go to follower
                  try to append entries

                  hmmm, this can work, but isn't it a bit dangerous?
                  'safest' thing to do is inject an electiontimeout here...
                  no! that can lead to other troubles ...; become follower!
                *)
               [`ToFollower;
                 `AppendEntries ae;]
          end
       | AppendEntriesResponse _ ->
          (* ignore *)
          []
       | RequestVoteResponse _ ->
          (* TODO collect and track these in the candidate state. *)
          [`ToLeader { next_index = Hashtbl.create 3; match_index = Hashtbl.create 3; }]
       | RequestVote _ ->
          begin
            let vote_response granted =
              `Send (from, (RequestVoteResponse { vote_granted = granted; })) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [vote_response false]
            | GT ->
               (* newer term, so no leader yet ... grant this one *)
               [`UpdateTerm (Some from, term);
                vote_response true]
            | EQ ->
               (* already voted for myself -> deny *)
               [vote_response false]
          end
     end

let leader_handle_event state ls = function
  | HeartbeatTimeout _ ->
     (* TODO check time? + renew 'lease' *)
     []
  | ElectionTimeout _ ->
     (* ignore, only relevant for candidates *)
     []
  | NodeMessage (from, (term, msg)) ->
     begin
       match msg with
       | AppendEntriesRequest ae ->
          begin
            let response success =
              `Send (from, (AppendEntriesResponse { success; })) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [response false]
            | GT ->
               (* TODO
                  go to follower
                  try to append entries
                  process original message in follower state? -> that's not such a bad idea...
                *)
               [`UpdateTerm (Some from, term);
                `ToFollower]
            | EQ ->
               failwith "impossible"
          end
       | AppendEntriesResponse aer ->
          (* TODO update leader state, learn about commit index advancing? *)
          []
       | RequestVoteResponse _ ->
          (* ignore ... hmm or look at term? *)
          []
       | RequestVote { last_log_index; last_log_term } ->
          begin
            let response vote_granted =
              `Send (from, (RequestVoteResponse { vote_granted; })) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [response false]
            | GT ->
               (* TODO bump term and go to follower *)
               [response true]
            | EQ ->
               [response false]
          end
     end

let handle_event state role event =
  match role with
  | Follower fs ->
     follower_handle_event state fs event
  | Candidate ->
     candidate_handle_event state event
  | Leader ls ->
     leader_handle_event state ls event

let produce_event () =
  Lwt.return
    (NodeMessage
       ("node_0", (0L, RequestVote
                         { last_log_index = None;
                           last_log_term = None; })))

let rec handle_effects state role = function
  | [] -> Lwt.return (state, role)
  | eff :: tl ->
     begin
       match eff with
       | `PromoteToCandidate ->
          (* go from follower to candidate.
             immediately hand electiontimeout event to candidate *)
          handle_effects state Candidate tl
       | `ToFollower ->
          let latest_heartbeat = Unix.gettimeofday () in
          (* TODO schedule timeout message ... *)
          handle_effects state (Follower { latest_heartbeat;}) tl
       | `ToLeader s ->
          handle_effects state (Leader s) tl
       | `Send (node, msg) ->
          (* plug in networking.. *)
          handle_effects state role tl
       | `UpdateTerm ((node : string option), (t : term)) ->
          (* if node is set then this must be fsync wise persisted *)
          handle_effects state role tl
       | `AppendEntries { prev_log_index; prev_log_term; entries; leader_commit } ->
          (* this might need to throw away part of the tlog.. *)
          handle_effects state role tl
     end

let rec event_loop state role =
  produce_event () >>= fun event ->
  let effects = handle_event state role event in
  handle_effects state role effects >>= fun (state, role) ->
  event_loop state role
