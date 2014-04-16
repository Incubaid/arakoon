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
open Lwt_buffer

type node_id = string
type term = Int64.t
type index = Int64.t
type entry = string (* consensus on opaque strings *)

type request_vote_request = {
  last_log_index : index option;
  last_log_term : index option;
}
type request_vote_response = {
  vote_granted : bool;
}

type append_entries_request = {
  predecessor : (index * term) option;
  entries : entry list;
  leader_commit : index;
}
type append_entries_response =
  | Success of index
  | OldTerm
  | MisMatch of index * term

type message_base =
  | AppendEntriesRequest of append_entries_request
  | AppendEntriesResponse of append_entries_response
  | RequestVote of request_vote_request
  | RequestVoteResponse of request_vote_response

type message = term * message_base

type event =
  | NodeMessage of node_id * message
  | Timeout of float (* time of timeout start *)
  (* | ClientUpdates of entry list *)


class type transaction_log = object
  (* method get_entry : index -> (entry * term) option Lwt.t *)
  (* method get_max_i : unit -> index option (\* should be cached *\) *)
  method append_entries : (index * term) option ->
                          term -> entry list ->
                          append_entries_response Lwt.t
end

type follower_state = unit

type 'a set = ('a, unit) Hashtbl.t

type candidate_state = {
  votes : node_id set;
}

module Int64Map = Map.Make(Int64)

type leader_state = {
  next_index : (node_id, index) Hashtbl.t;
  match_index : (node_id, index) Hashtbl.t;
  mutable rev_match_index : int Int64Map.t;
}

type role =
  | Follower of follower_state
  | Candidate of candidate_state
  | Leader of leader_state

type persisted_state = {
  current_term : term;
  voted_for : node_id option;
  log : transaction_log;
}

type state = {
  persisted_state : persisted_state;
  last_timeout : float;
  commit_index : index option;
}

type buffers = {
  timeout_buffer : event Lwt_buffer.t;
  node_buffer : event Lwt_buffer.t;
}

type raft_cfg = {
  timeout : float;
  others : node_id list;
  me : node_id;
  quorum : int;
  buffers : buffers;
}

let ignore () = []

let follower_handle_event cfg state fs = function
  | Timeout t ->
     if state.last_timeout <= t then
       [`PromoteToCandidate]
     else
       ignore () (* old heartbeat timeout *)
  | NodeMessage (from, (term, msg)) ->
     begin
       match msg with
       | AppendEntriesRequest ae ->
          begin
            let response aer =
              `Send (from, (AppendEntriesResponse aer)) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [response OldTerm]
            | GT ->
               [`UpdateTerm (Some from, term);
                `AppendEntriesAndReply (from, term, ae);]
            | EQ ->
               [`AppendEntriesAndReply (from, term, ae); ]
          end
       | AppendEntriesResponse _
       | RequestVoteResponse _ ->
          ignore ()
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

let candidate_handle_event cfg state cs = function
  | Timeout t ->
     if state.last_timeout <= t
     then
       (* TODO bump term
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
            let response aer =
              `Send (from, (AppendEntriesResponse aer)) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [response OldTerm]
            | GT ->
               [`UpdateTerm (Some from, term);
                `ToFollower;
                `AppendEntriesAndReply (from, term, ae)]
            | EQ ->
               [`ToFollower;
                `AppendEntriesAndReply (from, term, ae);]
          end
       | AppendEntriesResponse _ ->
          ignore ()
       | RequestVoteResponse { vote_granted; } ->
          begin
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               ignore ()
            | GT ->
               (* this is weird. anyway, bump term. *)
               [`UpdateTerm (None, term);
                `ToFollower]
            | EQ ->
               (* add vote if not yet in set *)
               if (Hashtbl.mem cs.votes from)
               then
                 (* already voted *)
                 ignore ()
               else
                 begin
                   Hashtbl.add cs.votes from ();
                   if Hashtbl.length cs.votes = cfg.quorum
                   then
                     [`ToLeader { next_index = Hashtbl.create 3;
                                  match_index = Hashtbl.create 3;
                                  rev_match_index = Int64Map.empty; }]
                   else
                     ignore ()
                 end
          end
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
                `ToFollower;
                vote_response true]
            | EQ ->
               (* already voted for myself -> deny *)
               [vote_response false]
          end
     end

let leader_handle_event cfg state ls = function
  | Timeout _ ->
     (* TODO check time? + renew 'lease' *)
     []
  | NodeMessage (from, (term, msg)) ->
     begin
       match msg with
       | AppendEntriesRequest ae ->
          begin
            let response aer =
              `Send (from, (AppendEntriesResponse aer)) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [response OldTerm]
            | GT ->
               [`UpdateTerm (Some from, term);
                `ToFollower;
                `AppendEntriesAndReply (from, term, ae)]
            | EQ ->
               failwith "impossible"
          end
       | AppendEntriesResponse aer ->
          begin
            match aer with
            | Success i ->
               let find' k m =
                 try
                   Int64Map.find k m
                 with Not_found -> 0 in
               let () =
                 try
                   let prev_i = Hashtbl.find ls.match_index from in
                   Hashtbl.replace ls.match_index from i;
                   let r' = Int64Map.add prev_i
                                         ((Int64Map.find prev_i ls.rev_match_index) - 1)
                                         ls.rev_match_index in
                   let r'' = Int64Map.add i
                                          ((find' i r') + 1)
                                          r' in
                   ls.rev_match_index <- r''
                 with Not_found ->
                   (* node does not yet exist in ls.match_index and ls.rev_match_index *)
                   Hashtbl.add ls.match_index from i;
                   ls.rev_match_index <- Int64Map.add i
                                                      ((find' i ls.rev_match_index) + 1)
                                                      ls.rev_match_index
               in

               (* determine possibly new(bumped) commit index *)
               let module C = Int64Map.Cursor in
               let count = ref 0 in
               let index =
                 C.with_cursor
                   ls.rev_match_index
                   (fun cur ->
                    while (C.next cur && !count < cfg.quorum) do
                      count := !count + snd (C.get cur)
                    done;
                    fst (C.get cur)) in
               if !count >= cfg.quorum
               then
                 let commit_index = index in
                 match state.commit_index with
                 | None ->
                    [`CommitIndex commit_index]
                 | Some i when Int64.(commit_index > i) ->
                    [`CommitIndex commit_index]
                 | _ ->
                   []
               else
                 []
            | OldTerm ->
               [`UpdateTerm (None, term);
                `ToFollower]
            | MisMatch (i, t) ->
               [`Catchup (from, i, t)]
          end
       | RequestVoteResponse _ ->
          (* ignore *)
          []
       | RequestVote { last_log_index; last_log_term } ->
          begin
            let vote_response vote_granted =
              `Send (from, (RequestVoteResponse { vote_granted; })) in
            match Int64.compare' term state.persisted_state.current_term with
            | LT ->
               [vote_response false]
            | GT ->
               [`UpdateTerm (Some from, term);
                `ToFollower;
                vote_response true]
            | EQ ->
               [vote_response false]
          end
     end

let handle_event cfg state role event =
  match role with
  | Follower fs ->
     follower_handle_event cfg state fs event
  | Candidate cs ->
     candidate_handle_event cfg state cs event
  | Leader ls ->
     leader_handle_event cfg state ls event

let produce_event buffers =
  let ordered_buffers = [ buffers.timeout_buffer;
                          buffers.node_buffer; ] in
  Lwt.pick (List.map
              Lwt_buffer.wait_for_item
              ordered_buffers) >>= fun () ->

  let rec inner = function
    | [] ->
       failwith "impossible produce event"
    | b :: tl ->
       if Lwt_buffer.has_item b
       then
         Lwt_buffer.take b
       else
         inner tl
  in
  inner ordered_buffers

let rec handle_effects cfg state role = function
  | [] -> Lwt.return (state, role)
  | eff :: tl ->
     begin
       match eff with
       | `PromoteToCandidate ->
          Lwt_buffer.add (Timeout state.last_timeout) cfg.buffers.timeout_buffer >>= fun () ->
          handle_effects cfg state (Candidate { votes = Hashtbl.create cfg.quorum }) tl
       | `ToFollower ->
          let last_timeout = Unix.gettimeofday () in
          Lwt.ignore_result
            (Lwt_unix.timeout cfg.timeout >>= fun () ->
             Lwt_buffer.add (Timeout last_timeout) cfg.buffers.timeout_buffer);
          handle_effects cfg {state with last_timeout } (Follower ()) tl
       | `ToLeader s ->
          handle_effects cfg state (Leader s) tl
       | `Send (node, msg) ->
          (* plug in networking.. *)
          handle_effects cfg state role tl
       | `UpdateTerm ((node : string option), (t : term)) ->
          (* TODO if node is set then this must be fsync wise persisted *)
          let state' = {state with
                         persisted_state = { state.persisted_state with
                                             current_term = t;
                                             voted_for = node;}} in
          handle_effects cfg state' role tl
       | `AppendEntriesAndReply (node,
                                 term,
                                 { predecessor;
                                   entries;
                                   leader_commit; }) ->
          let tlog = state.persisted_state.log in
          tlog # append_entries predecessor term entries >>= fun aer ->
          let msg_effect = (`Send (node, AppendEntriesResponse aer)) in
          handle_effects cfg state role (msg_effect :: tl)
       | `CommitIndex i ->
          (* TODO notify application + other nodes *)
          handle_effects cfg {state with commit_index = (Some i) } role tl
       | `Catchup (node, prev_i, prev_term) ->
          (* TODO find out from which point we should send data to this follower *)
          handle_effects cfg state role tl
     end

let rec event_loop cfg state role =
  produce_event cfg.buffers >>= fun event ->
  let effects = handle_event cfg state role event in
  handle_effects cfg state role effects >>= fun (state, role) ->
  event_loop cfg state role

(*
TODO

- timeouts

- fake messaging
- tests with a couple of nodes
 *)
