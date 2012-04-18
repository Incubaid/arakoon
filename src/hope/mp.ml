open Core

let _log m = Lwt.ignore_result(Lwt_io.printf m)
 
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
    | M_PREPARE of node_id * tick * tick
    | M_PROMISE of node_id * tick * tick * update option
    | M_NACK of node_id * tick * tick
    | M_ACCEPT of node_id * tick * tick * update
    | M_ACCEPTED of node_id * tick * tick
    | M_LEASE_TIMEOUT of tick
    | M_CLIENT_REQUEST of request_awakener * update
    | M_MASTERSET of node_id * tick

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
    | M_MASTERSET (m, n) ->
      Printf.sprintf "M_MASTERSET (n: %s) (m: %s)" (tick2s n) m
 
  let string_of_msg msg = 
    let buf = Buffer.create 8 in
    begin
      match msg with
      | M_PREPARE (src, (TICK n), (TICK i)) ->
        Llio.int_to buf 1; 
        Llio.string_to buf src;  
        Llio.int64_to buf n;
        Llio.int64_to buf i;
      | M_PROMISE (src, (TICK n), (TICK i), m_upd) ->
        begin
          Llio.int_to buf 2; 
          Llio.string_to buf src;
          Llio.int64_to buf n;
          Llio.int64_to buf i;
          begin
            match m_upd with 
              | None -> Llio.bool_to buf false
              | Some upd -> 
                Llio.bool_to buf true;
                update_to buf upd   
          end;
        end
      | M_NACK (src, (TICK n), (TICK i)) -> 
        Llio.int_to buf 3;
        Llio.string_to buf src;
        Llio.int64_to buf n;
        Llio.int64_to buf i
      | M_ACCEPT (src, (TICK n), (TICK i), upd) ->
        Llio.int_to buf 4;
        Llio.string_to buf src;
        Llio.int64_to buf n;
        Llio.int64_to buf i;
        update_to buf upd  
      | M_ACCEPTED (src, (TICK n), (TICK i)) ->
        Llio.int_to buf 5;
        Llio.string_to buf src;
        Llio.int64_to buf n;
        Llio.int64_to buf i 
      | M_MASTERSET (src, TICK n) ->
        Llio.int_to buf 6;
        Llio.string_to buf src;
        Llio.int64_to buf n
      | M_LEASE_TIMEOUT n -> failwith "lease timeout message cannot be serialized"
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
        let src, n, i, off = get_src_n_i off in
        M_PREPARE (src, (TICK n), (TICK i))
      | 2 ->
        let src, n, i, off = get_src_n_i off in
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
        M_PROMISE (src, (TICK n), (TICK i), m_up)
      | 3 -> 
        let src, n, i, off = get_src_n_i off in
        M_NACK (src, (TICK n), (TICK i))
      | 4 -> 
        let src, n, i, off = get_src_n_i off in
        let u, off = update_from str off in
        M_ACCEPT (src, (TICK n), (TICK i), u)
      | 5 -> 
        let src, n, i, off = get_src_n_i off in
        M_ACCEPTED (src, (TICK n), (TICK i))
      | 6 ->
        let (src, off) = Llio.string_from str off in
        let (n, off) = Llio.int64_from str off in
        M_MASTERSET (src, (TICK n))
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
  (* let ch_node_and_timeout = [CH_NODE; CH_TIMEOUT] *)
  let ch_node             = [CH_NODE]
  
  type election_votes = 
  {
    nnones : node_id list;
    nsomes : (update * node_id list) list;
  }
  
  type state =
  {
    round             : tick;
    proposed          : tick;
    accepted          : tick;
    state_n           : state_name;
    master_id         : node_id option;
    prop              : update option;
    votes             : node_id list;
    election_votes    : election_votes;
    now               : float;
    lease_expiration  : float;
    constants         : paxos_constants;
    cur_cli_req       : request_awakener option;
    valid_inputs      : msg_channel list;
  }
  
  let build_state c n p a u =
    {
      round = n;
      proposed = p;
      accepted = a;
      state_n = S_CLUELESS;
      master_id = None;
      prop = u;
      votes = [];
      election_votes = {nnones = []; nsomes = []};
      now = 0.0;
      lease_expiration = 0.0;
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
    Printf.sprintf "id: %s, n: %s, p: %s, a: %s, state: %s, master: %s, lease_expiry: %f, now: %f, votes: %d"
          s.constants.me (tick2s s.round) (tick2s s.proposed) (tick2s s.accepted) 
          (state_n2s s.state_n) (so2s s.master_id) s.lease_expiration s.now (List.length s.votes)
  
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
       
  type action =
    | A_RESYNC of node_id 
    | A_SEND_MSG of message * node_id
    | A_BROADCAST_MSG of message
    | A_COMMIT_UPDATE of tick * update * Core.result Lwt.u option
    | A_LOG_UPDATE of tick * update
    | A_START_TIMER of tick * float
    | A_CLIENT_REPLY of client_reply
    | A_STORE_LEASE of node_id * float

  let action2s = function
    | A_RESYNC src -> 
      Printf.sprintf "A_RESYNC (from: %s)" src 
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
    | A_STORE_LEASE (m, e) ->
      Printf.sprintf "A_STORE_LEASE (m:%s) (e:%f)" m e
      
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
    StepSuccess( [a], state )
    
  let send_promise state src n i =
    begin
    if is_lease_active state && not (is_node_master src state) && false
    then
      StepSuccess( [], state )
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
          state_n = new_state_n;
          valid_inputs = ch_all;
        }
        in
        StepSuccess( 
          A_SEND_MSG (reply_msg, src) :: 
          action_tail, 
        new_state )
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
          send_nack state.constants.me state.round state.accepted src state
      | (N_AHEAD, P_ACCEPTABLE, _) 
      | ( _, P_AHEAD, _ ) ->
        begin
          send_nack state.constants.me state.round state.accepted src state
        end
      | ( _, P_BEHIND, _) ->
        begin
          StepSuccess ([A_RESYNC src], state)
        end

  let get_updated_votes votes vote =
    let cleaned_votes = List.filter (fun v -> v <> vote) votes in
    vote :: cleaned_votes 
    
  let get_updated_prop state = function 
    | Some u -> Some u
    | None -> state.prop

  let build_accept_actions state n i u =
    let msg = M_ACCEPT(state.constants.me, n, i, u) in
    List.fold_left (fun acc n -> (A_SEND_MSG(msg, n)) :: acc) 
       [] (state.constants.me :: state.constants.others) 
  
  let bcast_mset_actions state =
    let msg = M_MASTERSET (state.constants.me, state.round) in
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
       
  let handle_promise n i m_upd src state =
    let diff = state_cmp n i state in
    match diff with
      | (_, P_BEHIND, _) -> StepFailure "Got promise from more recent node."
      | (N_BEHIND, _, _) -> StepSuccess([], state)
      | (N_AHEAD, _, _) ->  StepSuccess([], state)
      | (N_EQUAL, _, _) ->
        begin
          match state.state_n with
            | S_RUNNING_FOR_MASTER ->
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
                        let input_chs, postmortem_actions = 
		                      begin
		                        match update with
		                          | None -> ch_all , [] 
		                          | Some u -> 
		                            let actions = build_accept_actions state n i u in
		                            ch_node, actions
		                      end
		                    in 
		                    let new_state = { 
		                       state with 
		                       state_n = S_MASTER;
		                       election_votes = {nnones=[]; nsomes=[]};
                           votes = [];
		                       prop = update;
		                       valid_inputs = input_chs;
		                    } in
		                    let actions = 
		                      (bcast_mset_actions state)
		                      @
		                      (A_STORE_LEASE (state.constants.me, (state.now +. state.constants.lease_duration))
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
            | S_MASTER   -> StepSuccess ([], state) (* already reached consensus *) 
            | S_CLUELESS -> StepFailure "I'm clueless so I did not send prepare for this n"
        end 
  
  let handle_nack n i src state =
    let diff = state_cmp n i state in
    begin
        match diff with
        | (_, _, A_BEHIND) 
        | (_, _, A_COMMITABLE) -> 
          StepSuccess([A_RESYNC src], state)
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
    
  let handle_accept n i update src state =
    let diff = state_cmp n i state in
    begin
      match diff with
        | (N_BEHIND , _            , _)
        | (_        , P_BEHIND     , _) -> StepSuccess([A_RESYNC src], state)
        | (_        , P_AHEAD      , _)  
        | (N_AHEAD  , _            , _) -> StepSuccess([], state)
        | (N_EQUAL  , P_ACCEPTABLE , _) -> 
          begin
            match state.state_n with
              | S_MASTER
              | S_SLAVE ->
                let delayed_commit = extract_uncommited_action state i in
                let accept_update = A_LOG_UPDATE (i, update) in
                let accepted_msg = M_ACCEPTED (state.constants.me, n, i) in
                let send_accepted = A_SEND_MSG(accepted_msg, src) in
                let new_prop = Some update in
                let new_state = {
                  state with
                  prop = new_prop;
                } 
                in
                StepSuccess( (send_accepted :: accept_update :: delayed_commit), new_state)
              | _ -> StepFailure "Got accept with correct n and i dont know what to do with it"
          end
    end
  
  let handle_accepted n i src state =
    let diff = state_cmp n i state in
    begin
      match diff with
        | (_      , _, A_BEHIND) -> StepFailure "Got an Accepted for future i"
        | (_      , _, A_AHEAD)  
        | (N_AHEAD, _, A_COMMITABLE) -> StepSuccess([], state)
        | (N_EQUAL, _, A_COMMITABLE) -> 
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
    end
  
  type elections_needed =
    | LEASE_NEW
    | LEASE_EXTEND
    | LEASE_NOP


  let should_elections_start state =
    if state.now > state.lease_expiration 
    then LEASE_NEW
    else
      begin
        match state.master_id with
          | Some m when m = state.constants.me -> LEASE_EXTEND 
          | _ -> LEASE_NOP   
      end
      
    
  
  let start_elections new_n state = 
    let msg = M_PREPARE (state.constants.me, new_n, (next_tick state.accepted)) in
    let new_state = {
      state with
      round = new_n;
      master_id = Some state.constants.me;
      state_n = S_RUNNING_FOR_MASTER;
      votes = [];
      election_votes = {nnones=[]; nsomes=[]};
    } in
    let lease_expiry = build_lease_timer new_n (state.constants.lease_duration /. 2.0 ) in
    StepSuccess([A_BROADCAST_MSG msg; lease_expiry], new_state)

  let handle_lease_timeout n state =
    let diff = state_cmp n start_tick state in
    begin
      match diff with
        | (N_BEHIND, _, _) -> StepSuccess([], state)
        | (N_AHEAD , _, _) -> StepSuccess([], state)
        | (N_EQUAL , _, _) ->
          begin
            match should_elections_start state
            with
              | LEASE_NEW ->
                begin
                  let new_n = next_n n state.constants.node_cnt state.constants.node_ix in
                  start_elections new_n state
                end
              | LEASE_EXTEND ->
                begin
                  let d = state.constants.lease_duration  in
                  let l = A_STORE_LEASE (state.constants.me, state.now +. d) in
                  let t = A_START_TIMER (state.round, d /. 2.0) in
                  let actions = l :: t :: (bcast_mset_actions state) in
                  StepSuccess (actions,state)
                end
              | LEASE_NOP -> StepSuccess([], state)
          end
    end

  let handle_client_request w upd state =
    begin
      match state.state_n with
        | S_MASTER ->
          begin
            match state.cur_cli_req with
              | None -> 
                begin
                  let actions = build_accept_actions state state.round (next_tick state.accepted) upd in 
                  let new_state = {
                    state with
                    cur_cli_req = Some w;
                    valid_inputs = ch_node;
                    votes = [];
                    prop = Some upd;
                  } in 
                  StepSuccess (actions, new_state)
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
            match state.cur_cli_req with
              | None -> StepSuccess([], state)
              | Some w -> StepSuccess([A_CLIENT_REPLY reply], state)
          end
    end

  let handle_masterset n src state =
    begin
      match state.state_n with
        | S_SLAVE ->
          begin
            if state.round = n 
            then
              let d = state.constants.lease_duration in
              let l = A_STORE_LEASE (src, state.now +. d) in
              let t = A_START_TIMER (n, d) in
              StepSuccess ([l;t], state)
            else
              StepSuccess ([], state)
          end
        | _ -> StepSuccess([], state) 
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
      | M_MASTERSET (src, n) -> handle_masterset n src state
end 

module type MP_ACTION_DISPATCHER = sig
  type t
  val dispatch : t -> MULTI.state -> MULTI.action -> MULTI.state Lwt.t
  val get : t -> Core.k -> Core.v Lwt.t
  val get_meta : t -> string option Lwt.t
  val last_entries : t -> Core.tick -> Llio.lwtoc -> unit Lwt.t
end
