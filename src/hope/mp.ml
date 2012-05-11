open Core

let _log m = Lwt.ignore_result(Lwtc.log m)
 
module MULTI = struct
  type state_name = 
    | S_CLUELESS
    | S_RUNNING_FOR_MASTER
    | S_MASTER
    | S_SLAVE
  
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
    | M_PREPARE of node_id * tick * tick * tick
    | M_PROMISE of node_id * tick * tick * tick * update option
    | M_NACK of node_id * tick * tick * tick
    | M_ACCEPT of node_id * tick * tick * tick * update
    | M_ACCEPTED of node_id * tick * tick * tick
    | M_LEASE_TIMEOUT of tick * tick
    | M_CLIENT_REQUEST of request_awakener * update
    | M_MASTERSET of node_id * tick * tick

  let msg2s = function 
    | M_PREPARE (src, n, m, i) -> 
      Printf.sprintf "M_PREPARE (src: %s) (n: %s) (m: %s) (i: %s)" src (tick2s n) (tick2s m) (tick2s i)
    | M_PROMISE (src, n, m, i, m_upd) ->
      Printf.sprintf "M_PROMISE (src: %s) (n: %s) (m: %s) (i: %s) (m_u: %s)"
          src (tick2s n) (tick2s m) (tick2s i) (option2s m_upd)
    | M_NACK (src, n, m, i) -> 
      Printf.sprintf "M_NACK (src: %s) (n: %s) (m: %s) (i: %s)" src (tick2s n) (tick2s m) (tick2s i)
    | M_ACCEPT (src, n, m, i, upd) ->
      Printf.sprintf "M_ACCEPT (src: %s) (n: %s) (m: %s) (i: %s) (u: %s)" 
          src (tick2s n) (tick2s m) (tick2s i) (update2s upd)
    | M_ACCEPTED (src, n, m, i) -> 
      Printf.sprintf "M_ACCEPTED (src: %s) (n: %s) (m: %s) (i: %s)" src (tick2s n) (tick2s m) (tick2s i)
    | M_LEASE_TIMEOUT (n, m) ->
      Printf.sprintf "M_LEASE_TIMEOUT (n: %s) (m: %s)" (tick2s n) (tick2s m)
    | M_CLIENT_REQUEST (w, u) ->
      Printf.sprintf "M_CLIENT_REQUEST (u: %s)" (update2s u)
    | M_MASTERSET (m_id, n, m) ->
      Printf.sprintf "M_MASTERSET (n: %s) (m: %s) (m_id: %s)" (tick2s n) (tick2s m) m_id
 
  let string_of_msg msg = 
    let buf = Buffer.create 8 in
    begin
      match msg with
      | M_PREPARE (src, (TICK n), (TICK m), (TICK i)) ->
        Llio.int_to buf 1; 
        Llio.string_to buf src;  
        Llio.int64_to buf n;
        Llio.int64_to buf m;
        Llio.int64_to buf i;
      | M_PROMISE (src, (TICK n), (TICK m), (TICK i), m_upd) ->
        begin
          Llio.int_to buf 2; 
          Llio.string_to buf src;
          Llio.int64_to buf n;
          Llio.int64_to buf m;
          Llio.int64_to buf i;
          begin
            match m_upd with 
              | None -> Llio.bool_to buf false
              | Some upd -> 
                Llio.bool_to buf true;
                update_to buf upd   
          end;
        end
      | M_NACK (src, (TICK n), (TICK m), (TICK i)) -> 
        Llio.int_to buf 3;
        Llio.string_to buf src;
        Llio.int64_to buf n;
        Llio.int64_to buf m;
        Llio.int64_to buf i
      | M_ACCEPT (src, (TICK n), (TICK m), (TICK i), upd) ->
        Llio.int_to buf 4;
        Llio.string_to buf src;
        Llio.int64_to buf n;
        Llio.int64_to buf m;
        Llio.int64_to buf i;
        update_to buf upd  
      | M_ACCEPTED (src, (TICK n), (TICK m), (TICK i)) ->
        Llio.int_to buf 5;
        Llio.string_to buf src;
        Llio.int64_to buf n;
        Llio.int64_to buf m;
        Llio.int64_to buf i 
      | M_MASTERSET (src, TICK n, TICK m) ->
        Llio.int_to buf 6;
        Llio.string_to buf src;
        Llio.int64_to buf n;
        Llio.int64_to buf m
      | M_LEASE_TIMEOUT (n, m) -> failwith "lease timeout message cannot be serialized"
      | M_CLIENT_REQUEST (w, u) -> failwith "client request cannot be serialized"
    end;
    Buffer.contents buf
    
  let msg_of_string str =
    let (kind, off) = Llio.int_from str 0 in
    let get_src_n_i off = 
        let (src, off) = Llio.string_from str off in
        let (n, off) = Llio.int64_from str off in
        let (i, off) = Llio.int64_from str off in
        (src, n, i, off)
    in
    match kind with
      | 1 ->
        let src, n, m, off = get_src_n_i off in
        let i, off = Llio.int64_from str off in
        M_PREPARE (src, (TICK n), (TICK m), (TICK i))
      | 2 ->
        let src, n, m, off = get_src_n_i off in
        let i, off = Llio.int64_from str off in
        let b, off = Llio.bool_from str off in
        let m_up =
          begin
            if b 
            then
              let u, off = update_from str off in
              Some u 
            else
              None
          end
        in 
        M_PROMISE (src, (TICK n), (TICK m), (TICK i), m_up)
      | 3 -> 
        let src, n, m, off = get_src_n_i off in
        let i, off = Llio.int64_from str off in
        M_NACK (src, (TICK n), (TICK m), (TICK i))
      | 4 -> 
        let src, n, m, off = get_src_n_i off in
        let i, off = Llio.int64_from str off in
        let u, off = update_from str off in
        M_ACCEPT (src, (TICK n), (TICK m), (TICK i), u)
      | 5 -> 
        let src, n, m, off = get_src_n_i off in
        let i, off = Llio.int64_from str off in
        M_ACCEPTED (src, (TICK n), (TICK m), (TICK i))
      | 6 ->
        let (src, off) = Llio.string_from str off in
        let (n, off) = Llio.int64_from str off in
        let (m, off) = Llio.int64_from str off in
        M_MASTERSET (src, (TICK n), (TICK m))
      | i -> failwith "Unknown message type. Aborting."


  type paxos_constants = 
  {
    me             : node_id;
    others         : node_id list;
    quorum         : int;
    node_ix        : int;
    node_cnt       : int;
    lease_duration : float; 
  }
  
  let build_mp_constants q n n_others lease ix node_cnt =
    {
      quorum = q;
      me = n;
      others = n_others;
      lease_duration = lease;
      node_ix = ix;
      node_cnt = node_cnt;
    } 
  
  type msg_channel =
    | CH_CLIENT
    | CH_NODE
    | CH_TIMEOUT
  
  let ch_all              = [CH_CLIENT; CH_NODE; CH_TIMEOUT]
  let ch_node_and_timeout = [CH_NODE; CH_TIMEOUT] 
  
  type election_votes = 
  {
    nnones : node_id list;
    nsomes : (update * node_id list) list;
  }
  
  type state =
  {
    round             : tick;
    extensions        : tick;
    proposed          : tick;
    accepted          : tick;
    state_n           : state_name;
    master_id         : node_id option;
    prop              : update option;
    votes             : node_id list;
    election_votes    : election_votes;
    constants         : paxos_constants;
    cur_cli_req       : request_awakener option;
    valid_inputs      : msg_channel list;
  }
  
  let build_state c n p a u =
    {
      round = n;
      extensions = start_tick;
      proposed = p;
      accepted = a;
      state_n = S_CLUELESS;
      master_id = None;
      prop = u;
      votes = [];
      election_votes = {nnones = []; nsomes = []};
      constants = c;
      cur_cli_req = None;
      valid_inputs = ch_all;
    }

  let state_n2s = function 
    | S_CLUELESS           -> "S_CLUELESS"
    | S_RUNNING_FOR_MASTER -> "S_RUNNING_FOR_MASTER"
    | S_MASTER             -> "S_MASTER"
    | S_SLAVE              -> "S_SLAVE"
  
  let state2s s =
    let m = so2s s.master_id in
    Printf.sprintf "id: %s, n: %s, m: %s, p: %s, a: %s, state: %s, master: %s, votes: %d"
          s.constants.me (tick2s s.round) (tick2s s.extensions) (tick2s s.proposed) (tick2s s.accepted) 
          (state_n2s s.state_n) m (List.length s.votes)
  
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
  
  type client_reply = request_awakener * result
  
  let client_reply2s = function
    | w, FAILURE(rc,msg) -> 
      Printf.sprintf "Reply (rc: %d) (msg: '%s')" 
          (Int32.to_int (Arakoon_exc.int32_of_rc rc)) msg
    | w, UNIT -> "Reply success (unit)"
    | w, VALUE v -> Printf.sprintf "Reply success (value)" 
       
  type action =
    | A_RESYNC of node_id * tick * tick
    | A_SEND_MSG of message * node_id
    | A_BROADCAST_MSG of message
    | A_COMMIT_UPDATE of tick * update * Core.result Lwt.u option
    | A_LOG_UPDATE of tick * update * Core.result Lwt.u option
    | A_START_TIMER of tick * tick * float
    | A_CLIENT_REPLY of client_reply
    | A_STORE_LEASE of node_id option

  let action2s = function
    | A_RESYNC (src, n, m) -> 
      Printf.sprintf "A_RESYNC (from: %s) (n: %s) (m: %s)" src (tick2s n) (tick2s m) 
    | A_SEND_MSG (msg, dest) ->
      Printf.sprintf "A_SEND_MSG (msg: %s) (dest: %s)" (msg2s msg) dest
    | A_BROADCAST_MSG msg ->
      Printf.sprintf "A_BROADCAST_MSG (msg: %s)" (msg2s msg)
    | A_COMMIT_UPDATE (i, upd, req_id) ->
      Printf.sprintf "A_COMMIT_UPDATE (i: %s) (u: %s) (req_id: %s)"
        (tick2s i) (update2s upd) (option2s req_id) 
    | A_LOG_UPDATE (i, upd, cli_req) ->
      Printf.sprintf "A_LOG_UPDATE (i: %s) (u: %s) (req_id: %s)"
         (tick2s i) (update2s upd) (option2s cli_req)
    | A_START_TIMER (n, m, d) ->
      Printf.sprintf "A_START_TIMER (n: %s) (m: %s) (d: %f)" (tick2s n) (tick2s m) d
    | A_CLIENT_REPLY rep ->
      Printf.sprintf "A_CLIENT_REPLY (r: %s)" (client_reply2s rep)
    | A_STORE_LEASE m ->
      Printf.sprintf "A_STORE_LEASE (m:%s)" (so2s m) 
      
  type step_result =
    | StepFailure of string
    | StepSuccess of action list * state

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
  
  let next_n n node_cnt node_ix =
    let (+) = Int64.add in
    let (-) = Int64.sub in
    let TICK n' = n in
    let node_cnt = Int64.of_int node_cnt in
    let node_ix = Int64.of_int node_ix in 
    let modulo = Int64.rem n' node_cnt in
    let n' = n' + node_cnt - modulo in
    TICK (n' + node_ix) 
  
  let prev_n n node_cnt node_ix =
    let (+) = Int64.add in
    let (-) = Int64.sub in
    let TICK n' = n in
    let node_cnt = Int64.of_int node_cnt in
    let node_ix = Int64.of_int node_ix in 
    let modulo = Int64.rem n' node_cnt in
    let n'' = n' - modulo in
    if n'' + node_ix < n'
    then 
      TICK (n'' + node_ix)
    else 
      TICK (n'' - node_cnt + node_ix) 
  
  let is_node_master node_id state = 
    begin
      match state.master_id with
        | Some m -> m = node_id
        | None -> false
      end
  
  let build_promise n m i state =
    let m_upd = 
      begin
        if state.proposed = state.accepted || state.proposed < i
        then
          None
        else
          state.prop
      end
    in
    M_PROMISE(state.constants.me, n, m, i, m_upd)
  
  let build_lease_timer n m d =
    A_START_TIMER (n, m, d) 
  
  let build_nack me n m i dest =
    let reply = M_NACK(me, n, m, i) in
    A_SEND_MSG(reply, dest)
  
  let send_nack me n m i dest state =
    let a = build_nack me n m i dest in
    StepSuccess( [a], state )
    
  let send_promise state src n m i =
    begin
    if (state.master_id <> None) && not (is_node_master src state) 
    then
      StepSuccess( [], state )
    else
      begin
        let reply_msg = build_promise n m i state in
        let new_state_n, action_tail, new_inputs = 
          begin
            if src = state.constants.me 
            then
              state.state_n, [], state.valid_inputs
            else
              S_SLAVE, [build_lease_timer n m state.constants.lease_duration], ch_all
          end
        in
        let new_state = {
          state with
          round = n;
          extensions = m;
          state_n = new_state_n;
          valid_inputs = new_inputs;
        }
        in
        StepSuccess( 
          A_SEND_MSG (reply_msg, src) :: 
          action_tail, 
        new_state )
      end
    end
       
  let handle_prepare n m i src state =
    let diff = state_cmp n i state in
    match diff with
      | (N_BEHIND, P_ACCEPTABLE, _) ->
        begin
          send_promise state src n m i
        end
      | (N_EQUAL, P_ACCEPTABLE, _) ->
        if Some src = state.master_id || state.master_id = None 
        then
          send_promise state src n m i
        else 
          send_nack state.constants.me state.round state.extensions state.accepted src state
      | (N_AHEAD, P_ACCEPTABLE, _) 
      | ( _, P_AHEAD, _ ) ->
        begin
          send_nack state.constants.me state.round state.extensions state.accepted src state
        end
      | ( _, P_BEHIND, _) ->
        begin
          StepSuccess ([A_RESYNC (src, n, m)], state)
        end

  let get_updated_votes votes vote =
    let cleaned_votes = List.filter (fun v -> v <> vote) votes in
    vote :: cleaned_votes 
    
  let get_updated_prop state = function 
    | Some u -> Some u
    | None -> state.prop

  let build_accept_action i u cli_req =
    let log_update = A_LOG_UPDATE (i, u, cli_req) in
    [log_update]
   
  
  let bcast_mset_actions state =
    let msg = M_MASTERSET (state.constants.me, state.round, state.extensions) in
    List.fold_left (fun a n -> A_SEND_MSG(msg, n) :: a) [] state.constants.others
 
  let is_update_equiv left right = 
    left = right
  
  let extract_best_update election_votes =
    let is_better (best_u, best_cnt, total_cnt) (u, vs) =
      begin
        let l = List.length vs in
        let tc' = total_cnt + l in
        if l > best_cnt
        then (Some u, l, tc')
        else (best_u, best_cnt, tc')
      end
    in
    let (max_u, max_cnt, total_cnt) = 
        List.fold_left is_better (None, 0, 0) election_votes.nsomes 
    in
    (max_u, max_cnt + (List.length election_votes.nnones), total_cnt)
    
  
  let update_election_votes election_votes src = function
    | None -> { election_votes with nnones = get_updated_votes election_votes.nnones src }
    | Some u ->
      let update_one acc (u', nlist) =
        let nlist' =
          begin
            if is_update_equiv u u' 
            then get_updated_votes nlist src 
            else nlist
          end
        in
        (u', nlist') :: acc
      in 
      let vs' = List.fold_left update_one []  election_votes.nsomes in
      if List.exists (fun (u',_) -> is_update_equiv u u') vs' 
      then 
        { election_votes with nsomes = vs' }
      else
        { election_votes with nsomes = (u, [src]) :: vs' }
       
  let handle_promise n m i m_upd src state =
    let diff = state_cmp n i state in
    match diff with
      | (_, P_BEHIND, _) -> StepFailure "Got promise from more recent node."
      | (N_BEHIND, _, _) 
      | (N_AHEAD, _, _) ->  StepSuccess([], state)
      | (N_EQUAL, _, A_COMMITABLE) when m = state.extensions ->
        begin
          match state.state_n with
            | S_RUNNING_FOR_MASTER 
            | S_MASTER ->
              begin
                let new_votes = update_election_votes state.election_votes src m_upd in
                let update, max_votes, total_votes = extract_best_update new_votes in
                if total_votes = state.constants.node_cnt && max_votes < state.constants.quorum
                then
                  StepFailure "Impossible to reach consensus. Multiple updates without chance of reaching quorum"
                else
                  begin
                    if max_votes = state.constants.quorum
                    then
                      begin
                        (* We reached consensus on master *)
                        let postmortem_actions = 
		                      begin
		                        match update with
		                          | None -> [] 
		                          | Some u -> 
                                build_accept_action i u state.cur_cli_req 
		                      end
		                    in 
		                    let new_state = { 
		                       state with 
		                       state_n = S_MASTER;
		                       election_votes = {nnones=[]; nsomes=[]};
		                    } in
		                    let actions = 
		                      (bcast_mset_actions state)
		                      @
		                      (A_STORE_LEASE (Some state.constants.me )
		                      :: postmortem_actions)
		                    in
		                    StepSuccess(actions, new_state) 
		                  end
		                else
		                  let new_state = 
		                  {
		                    state with
		                    election_votes = new_votes;
		                  } in
		                  StepSuccess([], new_state)
                  end 
              end
            | S_SLAVE    -> StepFailure "I'm a slave and did not send prepare for this n"
            | S_CLUELESS -> StepFailure "I'm clueless so I did not send prepare for this n"
        end 
      | (N_EQUAL, _, _) ->  StepSuccess([], state)
  
  let handle_nack n m i src state =
    let diff = state_cmp n i state in
    begin
        match diff with
        | (_, _, A_BEHIND) 
        | (_, _, A_COMMITABLE) -> 
          StepSuccess([A_RESYNC (src, n, m)], state)
        | (_, _, A_AHEAD) -> StepSuccess([], state)
    end
  
  let extract_uncommited_action state new_i =
    begin 
      if state.accepted = state.proposed || state.proposed = new_i
      then
        []
      else
        begin
          match state.prop 
          with
            | None -> []
            | Some u -> 
              let new_accepted = next_tick state.accepted in
              [A_COMMIT_UPDATE (new_accepted, u, state.cur_cli_req)]
        end 
    end
    
  let handle_accept n m i update src state =
    let diff = state_cmp n i state in
    begin
      match diff with
        | (N_BEHIND , _            , _)
        | (_        , P_BEHIND     , _) -> StepSuccess([A_RESYNC (src, n, m)], state)
        | (_        , P_AHEAD      , _)  
        | (N_AHEAD  , _            , _) -> StepSuccess([], state)
        | (N_EQUAL  , P_ACCEPTABLE , _) -> 
          begin
            match state.state_n with
              | S_SLAVE ->
                let delayed_commit = extract_uncommited_action state i in
                let accept_update = A_LOG_UPDATE (i, update, None) in
                let accepted_msg = M_ACCEPTED (state.constants.me, n, state.extensions, i) in
                let send_accepted = A_SEND_MSG(accepted_msg, src) in
                let new_prop = Some update in
                let new_state = {
                  state with
                  prop = new_prop;
                } 
                in
                StepSuccess( List.rev(send_accepted :: accept_update :: delayed_commit), new_state)
              | _ -> StepFailure "Got accept with correct n and i dont know what to do with it"
          end
    end
  
  let handle_accepted n m i src state =
    let diff = state_cmp n i state in
    begin
      match diff with
        | (_      , _, A_BEHIND) -> StepFailure "Got an Accepted for future i"
        | (_      , _, A_AHEAD)  
        | (N_AHEAD, _, A_COMMITABLE) -> StepSuccess([], state)
        | (N_EQUAL, _, A_COMMITABLE) when m = state.extensions -> 
          begin
            let new_votes = get_updated_votes state.votes src in
            let (new_input_ch, new_prop, actions, new_cli_req, vs ) = 
              begin 
                let new_input_ch = state.valid_inputs in
                if List.length new_votes = state.constants.quorum
                then
                  match state.prop with
                    | None -> new_input_ch, state.prop, [], state.cur_cli_req, new_votes
                    | Some u ->
                       let n_accepted = next_tick state.accepted in
                       let ci = A_COMMIT_UPDATE (n_accepted, u, state.cur_cli_req) in
                       ch_all, None, [ci], None, []
                else
                  new_input_ch, state.prop, [], state.cur_cli_req, new_votes
              end
            in  
            let new_state = {
              state with
              votes        = vs;
              prop         = new_prop;
              cur_cli_req  = new_cli_req;
              valid_inputs = new_input_ch;
            }
            in
            StepSuccess(actions, new_state)
          end
        | (N_BEHIND, _, A_COMMITABLE) -> StepFailure "Accepted with future n, same i"
        | (N_EQUAL, _, A_COMMITABLE)  -> StepSuccess([], state)
    end

  let start_elections new_n m state = 
    let sn, mid = 
      begin
        match state.state_n with 
        | S_MASTER -> 
          if state.round = new_n 
          then (S_MASTER, Some state.constants.me)
          else (S_RUNNING_FOR_MASTER, None)
        | _ -> (S_RUNNING_FOR_MASTER, None)
      end
    in
    let msg = M_PREPARE (state.constants.me, new_n, m, (next_tick state.accepted)) in
    let new_state = {
      state with
      round = new_n;
      extensions = m;
      master_id = mid;
      state_n = sn;
      votes = [];
      election_votes = {nnones=[]; nsomes=[]};
    } in
    let lease_expiry = build_lease_timer new_n m (state.constants.lease_duration /. 2.0 ) in
    StepSuccess([A_STORE_LEASE mid; A_BROADCAST_MSG msg; lease_expiry], new_state)

  let handle_lease_timeout n m state =
    let diff = state_cmp n start_tick state in
    begin
      match diff with
        | (N_EQUAL , _, _) when m = state.extensions ->
          begin
            match state.master_id
            with
              | Some m when m = state.constants.me ->
                begin
                  let new_m = next_tick state.extensions in
                  start_elections state.round new_m state
                end
              
              | _  ->
                begin
                  let new_n = next_n n state.constants.node_cnt state.constants.node_ix in
                  start_elections new_n start_tick state
                end
          end
        | (_, _, _) -> StepSuccess([], state)
    end

  let handle_client_request w upd state =
    begin
      match state.state_n with
        | S_MASTER ->
          begin
            match state.cur_cli_req with
              | None -> 
                begin
                  let actions = build_accept_action (next_tick state.accepted) upd (Some w) in 
                  StepSuccess (actions, state)
                end
              | Some w ->
                begin 
                  let reply = (w, FAILURE(Arakoon_exc.E_UNKNOWN_FAILURE, 
                               "Cannot process request, already handling a client request")) 
                  in
                  StepSuccess ([A_CLIENT_REPLY reply], state)
                end
          end
        | _ ->
          begin
            let reply = (w, FAILURE(Arakoon_exc.E_NOT_MASTER, 
                                            "Cannot process request on none-master")) 
            in
            StepSuccess([A_CLIENT_REPLY reply], state)
          end
    end

  let handle_masterset n m src state =
    begin
      match state.state_n with
        | S_SLAVE when state.round = n ->
          begin
            let l = A_STORE_LEASE (Some src) in
            let t =
              if state.round = n && state.extensions = m 
              then []
              else
                let d = state.constants.lease_duration in
                [A_START_TIMER (n, m, d)]
            in 
            StepSuccess (l :: t, state)
          end
        | _ -> StepSuccess([], state) 
    end
    
  let step msg state =
    match msg with
      | M_PREPARE (src, n, m, i) -> handle_prepare n m i src state
      | M_PROMISE (src, n, m, i, m_upd) -> handle_promise n m i m_upd src state
      | M_NACK (src, n, m, i) -> handle_nack n m i src state
      | M_ACCEPT (src, n, m, i, upd) -> handle_accept n m i upd src state
      | M_ACCEPTED (src, n, m, i) -> handle_accepted n m i src state
      | M_LEASE_TIMEOUT (n, m) -> handle_lease_timeout n m state
      | M_CLIENT_REQUEST (w, upd) -> handle_client_request w upd state
      | M_MASTERSET (src, n, m) -> handle_masterset n m src state
end 

module type MP_ACTION_DISPATCHER = sig
  type t
  val dispatch : t -> MULTI.state -> MULTI.action -> MULTI.state Lwt.t
end
