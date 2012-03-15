open Core

let _log m = Lwt.ignore_result(Lwt_io.printf m)
 
module MULTI = struct
  type state_name = 
    | S_CLUELESS
    | S_RUNNING_FOR_MASTER
    | S_MASTER
    | S_SLAVE
    
  type tick = TICK of int
  
  let (<:) (TICK i0) (TICK i1) = i0 < i1

  let start_tick = TICK 0
  let next_tick (TICK i) = TICK (i+1)
  let add_tick (TICK l) (TICK r) = TICK (l+r)
  let tick2s (TICK t) = 
    Printf.sprintf "%d" t
  
  let option2s (x : 'a option)  = 
    match x with
      | Some _ -> "Some ..."
      | None -> "None"

   let so2s = function
    | None -> "None"
    | Some s -> Printf.sprintf "'%s'" s
  
  type request_awakener = Core.result Lwt.u 
  type node_id = string
    
  type message = 
    | M_PREPARE of node_id * tick * tick
    | M_PROMISE of node_id * tick * tick * update option
    | M_NACK of node_id * tick * tick
    | M_ACCEPT of node_id * tick * tick * update
    | M_ACCEPTED of node_id * tick * tick
    | M_LEASE_TIMEOUT of tick
    | M_CLIENT_REQUEST of request_awakener * update

  let msg2s = function 
    | M_PREPARE (src, n, i) -> 
      Printf.sprintf "M_PREPARE (src: %s) (n: %s) (i: %s)" src (tick2s n) (tick2s i)
    | M_PROMISE (src, n, i, m_upd) ->
      Printf.sprintf "M_PROMISE (src: %s) (n: %s) (i: %s) (m_u: %s)"
          src (tick2s n) (tick2s i) (option2s m_upd)
    | M_NACK (src, n, i) -> 
      Printf.sprintf "M_NACK (src: %s) (n: %s) (i: %s)" src (tick2s n) (tick2s i)
    | M_ACCEPT (src, n, i, upd) ->
      Printf.sprintf "M_ACCEPT (src: %s) (n: %s) (i: %s) (u: %s)" 
          src (tick2s n) (tick2s i) (update2s upd)
    | M_ACCEPTED (src, n, i) -> 
      Printf.sprintf "M_ACCEPTED (src: %s) (n: %s) (i: %s)" src (tick2s n) (tick2s i)
    | M_LEASE_TIMEOUT n ->
      Printf.sprintf "M_LEASE_TIMEOUT (n: %s)" (tick2s n)
    | M_CLIENT_REQUEST (w, u) ->
      Printf.sprintf "M_CLIENT_REQUEST (u: %s)" (update2s u)
 
  type paxos_constants = 
  {
    me             : node_id;
    quorum         : int;
    node_ix        : int;
    node_cnt       : int;
    lease_duration : float; 
  }
  
  type msg_channel =
    | CH_CLIENT
    | CH_NODE
    | CH_TIMEOUT
  
  let ch_all              = [CH_CLIENT; CH_NODE; CH_TIMEOUT]
  let ch_node_and_timeout = [CH_NODE; CH_TIMEOUT];
  
  type state =
  {
    round             : tick;
    proposed          : tick;
    accepted          : tick;
    state_n           : state_name;
    master_id         : node_id option;
    prop              : update option;
    votes             : node_id list;
    now               : float;
    lease_expiration  : float;
    constants         : paxos_constants;
    cur_cli_req       : request_awakener option;
    valid_inputs      : msg_channel list;
  }

  let state_n2s = function 
    | S_CLUELESS           -> "S_CLUELESS"
    | S_RUNNING_FOR_MASTER -> "S_RUNNING_FOR_MASTER"
    | S_MASTER             -> "S_MASTER"
    | S_SLAVE              -> "S_SLAVE"
  
  let state2s s =
    Printf.sprintf "id: %s, n: %s, i: %s, i': %s, state: %s, master: %s, lease_expiry: %f, now: %f"
          s.constants.me (tick2s s.round) (tick2s s.accepted) (tick2s s.proposed) 
          (state_n2s s.state_n) (so2s s.master_id) s.lease_expiration s.now
  
  type round_diff =
    | N_EQUAL
    | N_BEHIND
    | N_AHEAD

  type proposed_diff =
    | P_ACCEPTABLE
    | P_BEHIND
    | P_AHEAD
  
  type accepted_diff  =
    | A_COMMITABLE
    | A_BEHIND
    | A_AHEAD

  type state_diff = round_diff * proposed_diff * accepted_diff 
  
  type client_reply =
    | CLIENT_REPLY_FAILURE of request_awakener * Arakoon_exc.rc * string 
  
  let client_reply2s = function
    | CLIENT_REPLY_FAILURE (w, rc, msg) ->
      Printf.sprintf "Reply (rc: %d) (msg: '%s')" 
          (Int32.to_int (Arakoon_exc.int32_of_rc rc)) msg 
  type action =
    (* Another node with more recent updates was discovered *)
    (* future_n , future_i , node_id                        *)
    | A_RESYNC of tick * tick * node_id 
    | A_SEND_MSG of message * node_id
    | A_BROADCAST_MSG of message
    | A_COMMIT_UPDATE of tick * update * Core.result Lwt.u option
    | A_LOG_UPDATE of tick * update
    | A_START_TIMER of tick * float
    | A_CLIENT_REPLY of client_reply
    | A_DIE of string

  let action2s = function
    | A_RESYNC (n, i, src) -> 
      Printf.sprintf "A_RESYNC (n: %s) (i: %s) (from: %s)" (tick2s n) (tick2s i) src 
    | A_SEND_MSG (msg, dest) ->
      Printf.sprintf "A_SEND_MSG (msg: %s) (dest: %s)" (msg2s msg) dest
    | A_BROADCAST_MSG msg ->
      Printf.sprintf "A_BROADCAST_MSG (msg: %s)" (msg2s msg)
    | A_COMMIT_UPDATE (i, upd, req_id) ->
      Printf.sprintf "A_COMMIT_UPDATE (i: %s) (u: %s) (req_id: %s)"
        (tick2s i) (update2s upd) (option2s req_id) 
    | A_LOG_UPDATE (i, upd) ->
      Printf.sprintf "A_LOG_UPDATE (i: %s) (u: %s)"
         (tick2s i) (update2s upd)
    | A_START_TIMER (n, d) ->
      Printf.sprintf "A_START_TIMER (n: %s) (d: %f)" (tick2s n) d
    | A_CLIENT_REPLY rep ->
      Printf.sprintf "A_CLIENT_REPLY (r: %s)" (client_reply2s rep)
    | A_DIE msg ->
      Printf.sprintf "A_DIE (msg: '%s')" msg

  let state_cmp n i state =
    let c_diff =
      begin
        match state.round with 
          | n' when n > n' -> N_BEHIND
          | n' when n < n' -> N_AHEAD
          | n'             -> N_EQUAL
      end
    in
    let p_diff =
      begin
        match (next_tick state.proposed) with 
          | i' when i > i'             -> P_BEHIND
          | i' when i < state.proposed -> P_AHEAD
          | i' -> 
            if state.proposed = state.accepted 
            then
              if i = next_tick state.proposed 
              then
                P_ACCEPTABLE
              else
                P_AHEAD
            else
              P_ACCEPTABLE
      end
    in
    let a_diff = 
      begin 
        match next_tick state.accepted with
          | i' when i > i' -> A_BEHIND
          | i' when i < i' -> A_AHEAD
          | i' -> A_COMMITABLE
      end
    in
    (c_diff, p_diff, a_diff)
  
  let update_n n state =
    let TICK n' = n in
    let modulo = n' mod state.constants.node_cnt in
    let n' = n' + state.constants.node_cnt - modulo in
    TICK (n' + state.constants.node_ix) 
    
  let is_lease_active state =
    state.now < state.lease_expiration 
  
  let is_node_master node_id state = 
    begin
      match state.master_id with
        | Some m -> m = node_id
        | None -> false
      end
  
  let build_promise n i state =
    let m_upd = 
      begin
        if state.proposed = state.accepted || state.proposed < i
        then
          None
        else
          state.prop
      end
    in
    M_PROMISE(state.constants.me, n, i, m_upd)
  
  let build_lease_timer n d =
    A_START_TIMER (n, d) 
  
  let build_nack me n i dest =
    let reply = M_NACK(me, n, i) in
    A_SEND_MSG(reply, dest)
  
  let send_nack me n i dest state =
    let a = build_nack me n i dest in
    [a], state
    
  let send_promise state src n i =
    begin
    if is_lease_active state && not (is_node_master src state)
    then
      [], state
    else
      begin
        let reply_msg = build_promise n i state in
        let new_state_n, action_tail = 
          begin
            if src = state.constants.me 
            then
              state.state_n, []
            else
              S_SLAVE, [build_lease_timer n state.constants.lease_duration]
          end
        in
        let new_state = {
          state with
          round = n;
          master_id = Some src;
          state_n = new_state_n;
          valid_inputs = ch_all;
          lease_expiration  = state.now +. state.constants.lease_duration;
        }
        in
        A_SEND_MSG (reply_msg, src) :: action_tail, new_state
      end
    end
       
  let handle_prepare n i src state =
    let diff = state_cmp n i state in
    match diff with
      | (N_BEHIND, P_ACCEPTABLE, _) ->
        begin
          send_promise state src n i
        end
      | (N_EQUAL, P_ACCEPTABLE, _) ->
        if src = state.constants.me 
        then
          send_promise state src n i
        else 
          send_nack state.constants.me state.round (next_tick state.accepted) src state
      | (N_AHEAD, P_ACCEPTABLE, _) 
      | ( _, P_AHEAD, _ ) ->
        begin
          send_nack state.constants.me state.round (next_tick state.accepted) src state
        end
      | ( _, P_BEHIND, _) ->
        begin
          ([A_RESYNC(n, i, src)], state)
        end

  let get_updated_votes votes vote =
    let cleaned_votes = List.filter (fun v -> v <> vote) votes in
    vote :: cleaned_votes 
    
  let get_updated_prop state = function 
    | Some u -> Some u
    | None -> state.prop

  let handle_promise n i m_upd src state =
    let diff = state_cmp n i state in
    match diff with
      | (_, P_BEHIND, _) -> ([A_DIE "Got promise from more recent node."]), state
      | (N_BEHIND, _, _) -> ([A_DIE "Got promise from future."]), state
      | (N_AHEAD, _, _) ->  ([], state)
      | (N_EQUAL, _, _) ->
        begin
          match state.state_n with
            | S_RUNNING_FOR_MASTER ->
              begin
                let new_votes = get_updated_votes state.votes src in
                let new_prop = get_updated_prop state m_upd in
                if List.length new_votes = state.constants.quorum
                then
                  begin
                    (* We reached consensus on master *)
                    let input_chs, actions, new_votes = 
                      begin
                        match new_prop with
                          | None -> ch_all , [], []
                          | Some u -> 
                            let acc = M_ACCEPT(state.constants.me, n, i, u) in
                            ch_node_and_timeout, [A_BROADCAST_MSG acc], []
                      end
                    in 
                    let new_state = { 
                       state with 
                       state_n = S_MASTER;
                       votes = new_votes;
                       prop = new_prop;
                       valid_inputs = input_chs;
                    } in
                    (actions, new_state) 
                  end
                else
                  let new_state = 
                  {
                    state with
                    votes = new_votes;
                    prop  = new_prop;
                  } in
                  ([], new_state) 
              end
            | S_SLAVE    -> [A_DIE "I'm a slave and did not send prepare for this n"], state
            | S_MASTER   -> [], state (* alreayd reach quorum *) 
            | S_CLUELESS -> [A_DIE "I'm clueless so I did not send prepare for this n"], state
        end 
  
  let handle_nack n i src state =
    let diff = state_cmp n i state in
    begin
        match diff with
        | (_, P_BEHIND, _) -> 
          [A_RESYNC(n, i, src)], state
        | (_, P_AHEAD, _) -> [], state
        | (N_EQUAL, P_ACCEPTABLE, _) -> 
          let n' = update_n n state in
          let msg = M_PREPARE(state.constants.me, n',i) in
          let new_state = { 
            state with
            state_n = S_RUNNING_FOR_MASTER;
            votes = [];
            master_id = Some state.constants.me
          }
          in
          [A_BROADCAST_MSG msg], new_state
        | (N_AHEAD, P_ACCEPTABLE, _ ) ->
          [], state
        | (N_BEHIND, P_ACCEPTABLE, _ ) ->
          [], state
    end
  
  let extract_uncommited_action state new_i =
    begin 
      if state.accepted = state.proposed || state.proposed = new_i
      then
        [], state.accepted
      else
        begin
          match state.prop 
          with
            | None -> [], state.accepted
            | Some u -> 
              let new_accepted = next_tick state.accepted in
              [A_COMMIT_UPDATE (new_accepted, u, state.cur_cli_req)], new_accepted
        end 
    end
    
  let handle_accept n i update src state =
    let diff = state_cmp n i state in
    begin
      match diff with
        | (_, P_BEHIND, _) -> [A_RESYNC(n, i, src)], state
        | (_, P_AHEAD, _)  -> 
            let msg = Printf.sprintf "Got update for value of i that is already processed (accepted: %s > msg_i: %s)" 
              (tick2s state.accepted) (tick2s i) in 
            [A_DIE msg], state
        | (N_AHEAD, _, _ ) -> [], state
        | (N_BEHIND, _, _) -> [A_RESYNC(n, i, src)], state
        | (N_EQUAL, P_ACCEPTABLE, _) -> 
          begin
            match state.state_n with
              | S_MASTER
              | S_SLAVE ->
                let (delayed_commit, new_accepted) = extract_uncommited_action state i in
                let new_proposed = next_tick new_accepted in
                let accept_update = A_LOG_UPDATE (i, update) in
                let accepted_msg = M_ACCEPTED (state.constants.me, n, i) in
                let send_accepted = A_SEND_MSG(accepted_msg, src) in
                let new_prop = Some update in
                let new_state = {
                  state with
                  proposed = new_proposed;
                  accepted = new_accepted;
                  prop = new_prop;
                } 
                in
                (send_accepted :: accept_update :: delayed_commit), new_state
              | _ -> [A_DIE "Got accept with correct n and i dont know what to do with it"], state
          end
    end
  
  let handle_accepted n i src state =
    let diff = state_cmp n i state in
    begin
      match diff with
        | (_, _, A_BEHIND) -> [A_DIE "Got an Accepted for future i"], state
        | (_, _, A_AHEAD)  
        | (N_AHEAD, _, A_COMMITABLE) ->
            [], state
        | (N_EQUAL, _, A_COMMITABLE) -> 
          begin
            let new_votes = get_updated_votes state.votes src in
            let (new_input_ch, new_accepted, new_prop, actions, new_cli_req ) = 
              begin 
                let new_input_ch = state.valid_inputs in
                if List.length new_votes = state.constants.quorum
                then
                  match state.prop with
                    | None -> new_input_ch, state.accepted, state.prop, [], state.cur_cli_req
                    | Some u ->
                       let n_accepted = next_tick state.accepted in
                       let ci = A_COMMIT_UPDATE (n_accepted, u, state.cur_cli_req) in
                       ch_all, n_accepted, None, [ci], None
                else
                  new_input_ch, state.accepted, state.prop, [], state.cur_cli_req
              end
            in  
            let new_state = {
              state with
              accepted     = new_accepted;
              votes        = new_votes;
              prop         = new_prop;
              cur_cli_req  = new_cli_req;
              valid_inputs = new_input_ch;
            }
            in
            actions, new_state
          end
        | (N_BEHIND, _, A_COMMITABLE) -> [A_DIE "Accepted with future n, same i"], state
    end
  
  let should_elections_start state =
    state.now > state.lease_expiration
    ||
    begin
      match state.master_id with
        | Some m -> m = state.constants.me
        | None -> true (* No clue how this would be possible *)
    end
  
  let start_elections new_n state = 
    let msg = M_PREPARE (state.constants.me, new_n, (next_tick state.accepted)) in
    let new_state = {
      state with
      round = new_n;
      master_id = Some state.constants.me;
      state_n = S_RUNNING_FOR_MASTER;
      votes = [];
    } in
    let lease_expiry = build_lease_timer new_n (state.constants.lease_duration /. 2.0 ) in
    [A_BROADCAST_MSG msg; lease_expiry], new_state

  let handle_lease_timeout n state =
    let diff = state_cmp n start_tick state in
    begin
      match diff with
        | (N_BEHIND, _, _) -> [A_DIE "Got timeout from future lease"], state
        | (N_AHEAD, _, _) -> [], state
        | (N_EQUAL, _, _) ->
          begin
            if should_elections_start state
            then
              begin
                let new_n = update_n n state in
                start_elections new_n state
              end
            else
              let lease_timer = build_lease_timer n state.constants.lease_duration in
              [lease_timer], state
          end
    end

  let handle_client_request id upd state =
    begin
      match state.state_n with
        | S_MASTER ->
          begin
            match state.cur_cli_req with
              | None -> 
                begin
                  let c = state.constants in
                  let msg = M_ACCEPT (c.me, state.round, (next_tick state.accepted), upd) in
                  let new_state = {
                    state with
                    cur_cli_req = Some id;
                    valid_inputs = ch_node_and_timeout;
                    votes = [];
                  } in 
                  [A_BROADCAST_MSG msg], new_state
                end
              | Some _ ->
                begin 
                  let reply = CLIENT_REPLY_FAILURE (id, Arakoon_exc.E_UNKNOWN_FAILURE, 
                               "Cannot process request, already handling a client request") 
                  in
                  [A_CLIENT_REPLY reply], state
                end
          end
        | _ ->
          begin
            let reply = CLIENT_REPLY_FAILURE (id, Arakoon_exc.E_NOT_MASTER, 
                                            "Cannot process request on none-master") 
            in
            [A_CLIENT_REPLY reply], state
          end
    end

  let step msg state =
    match msg with
      | M_PREPARE (src, n, i) -> handle_prepare n i src state
      | M_PROMISE (src, n, i, m_upd) -> handle_promise n i m_upd src state
      | M_NACK (src, n, i) -> handle_nack n i src state
      | M_ACCEPT (src, n, i, upd) -> handle_accept n i upd src state
      | M_ACCEPTED (src, n, i) -> handle_accepted n i src state
      | M_LEASE_TIMEOUT n -> handle_lease_timeout n state
      | M_CLIENT_REQUEST (w, upd) -> handle_client_request w upd state
end 

module type MP_ACTION_DISPATCHER = sig
  type t
  val dispatch : t -> MULTI.state -> MULTI.action -> MULTI.state Lwt.t
end