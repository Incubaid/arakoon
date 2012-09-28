open Core

type node_id = string
type request_awakener = Core.result Lwt.u
type client_reply = request_awakener * result

(* "Show" helpers *)
let string_of_option f = function
  | None -> "None"
  | Some v -> Printf.sprintf "Some (%s)" (f v)

let string_of_list f l =
  let rec helper acc = function
    | [] -> ""
    | (h :: t) -> helper (acc ^ f h) t
  in
  "[" ^ helper "" l ^ "]"

let string_of_string = Printf.sprintf "\"%s\""

let string_of_pair f1 f2 (a, b) = Printf.sprintf "(%s, %s)" (f1 a) (f2 b)

let const a = fun _ -> a


module Lens = struct
  (* 'a is "record" type, 'b is "field" type *)
  type ('a, 'b) getter = 'a -> 'b
  type ('a, 'b) setter = 'a -> 'b -> 'a
  type ('a, 'b) lens = ('a, 'b) getter * ('a, 'b) setter

  let read (l : ('a, 'b) lens) (a : 'a) : 'b = (fst l) a
  let update (l : ('a, 'b) lens) (a : 'a) (b : 'b) : 'a = (snd l) a b

  (* Lens composition *)
  let (<.>) (b : ('b, 'c) lens) (a : ('a, 'b) lens) : ('a, 'c) lens =
    (fun v -> fst b (fst a v)), (fun v n -> (snd a) v (snd b (fst a v) n))
end
open Lens


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


module type TICK = sig
  type t

  val to_string : t -> string
  val encode : Buffer.t -> t -> unit
  val decode : string -> int -> (t * int)

  val to_int64 : t -> int64
  val succ : t -> t
end

module NTick : TICK = struct
  type t = int64

  let to_string = Int64.to_string
  let encode = Llio.int64_to
  let decode = Llio.int64_from
  let to_int64 x = x
  let succ = Int64.succ
end
module MTick : TICK = struct
  type t = int64

  let to_string = Int64.to_string
  let encode = Llio.int64_to
  let decode = Llio.int64_from
  let to_int64 x = x
  let succ = Int64.succ
end
module ITick : TICK = struct
  type t = int64

  let to_string = Int64.to_string
  let encode = Llio.int64_to
  let decode = Llio.int64_from
  let to_int64 x = x
  let succ = Int64.succ
end


module State = struct
  module StateName = struct
    type t = S_CLUELESS
           | S_RUNNING_FOR_MASTER
           | S_MASTER
           | S_SLAVE

    let to_string = function
      | S_CLUELESS -> "S_CLUELESS"
      | S_RUNNING_FOR_MASTER -> "S_RUNNING_FOR_MASTER"
      | S_MASTER -> "S_MASTER"
      | S_SLAVE -> "S_SLAVE"
  end

  module ElectionVotes = struct
    type t = { nones : node_id list
             ; somes : (update * node_id list) list
             }

    let to_string t =
      Printf.sprintf "ElectionVotes.t { nones=%s; somes=%s }"
      (string_of_list string_of_string t.nones)
      (string_of_list
        (string_of_pair string_of_update (string_of_list string_of_string))
        t.somes)
  end

  module PaxosConstants = struct
    type t = { _me : node_id
             ; _others : node_id list
             ; _quorum : int
             ; _node_ix : int
             ; _node_cnt : int
             ; _lease_duration : float
             }

    let to_string t =
      Printf.sprintf
        "PaxosConstants.t { _me=\"%s\"; _others=%s; _quorum=%i; _node_ix=%i; _lease_duration=%f }"
        t._me (string_of_list string_of_string t._others) t._quorum t._node_ix
        t._lease_duration

    let me = (fun v -> v._me), (fun v n -> { v with _me=n })
    let others = (fun v -> v._others), (fun v n -> { v with _others=n })
    let quorum = (fun v -> v._quorum), (fun v n -> { v with _quorum=n })
    let node_ix = (fun v -> v._node_ix), (fun v n -> { v with _node_ix=n })
    let node_cnt = (fun v -> v._node_cnt), (fun v n -> { v with _node_cnt=n })
    let lease_duration = (fun v -> v._lease_duration), (fun v n -> { v with _lease_duration=n })
  end

  module MessageChannel = struct
    type t = CH_CLIENT
           | CH_NODE
           | CH_TIMEOUT

    let to_string = function
      | CH_CLIENT -> "CH_CLIENT"
      | CH_NODE -> "CH_NODE"
      | CH_TIMEOUT -> "CH_TIMEOUT"
  end

  (* TODO Use NTick.t/MTick.t/ITick.t *)
  type tick = int64

  type t = { _round : tick
           ; _extensions : tick
           ; _proposed : tick
           ; _accepted : tick
           ; _state_name : StateName.t
           ; _master_id : node_id option
           ; _prop : update option
           ; _votes : node_id list
           ; _election_votes : ElectionVotes.t
           ; _constants : PaxosConstants.t
           ; _cur_cli_req : request_awakener option
           ; _valid_inputs : MessageChannel.t list
           }

  let to_string t =
    Printf.sprintf
      "State.t { _round=%s; _extensions=%s; _proposed=%s; _accepted=%s; _state_name=%s; _master_id=%s; _prop=%s; _votes=%s; _election_votes=%s; _constants=%s; _cur_cli_req=%s; _valid_inputs=%s }"
      (Int64.to_string t._round) (Int64.to_string t._extensions)
      (Int64.to_string t._proposed) (Int64.to_string t._accepted)
      (StateName.to_string t._state_name)
      (string_of_option string_of_string t._master_id)
      (string_of_option string_of_update t._prop)
      (string_of_list string_of_string t._votes)
      (ElectionVotes.to_string t._election_votes)
      (PaxosConstants.to_string t._constants)
      (string_of_option (const "<request_awakener>") t._cur_cli_req)
      (string_of_list MessageChannel.to_string t._valid_inputs)

  let round = (fun s -> s._round), (fun s r -> { s with _round=r })
  let extensions = (fun s -> s._extensions), (fun s e -> { s with _extensions=e })
  let proposed = (fun s -> s._proposed), (fun s p -> { s with _proposed=p })
  let accepted = (fun s -> s._accepted), (fun s a -> { s with _accepted=a })
  let state_name : (t, StateName.t) lens =
    (fun s -> s._state_name), (fun s n -> { s with _state_name=n })
  let master_id = (fun s -> s._master_id), (fun s m -> { s with _master_id=m })
  let prop = (fun s -> s._prop), (fun s p -> { s with _prop=p })
  let votes = (fun s -> s._votes), (fun s v -> { s with _votes=v })
  let election_votes = (fun s -> s._election_votes), (fun s e -> { s with _election_votes=e })
  let constants = (fun s -> s._constants), (fun s c -> { s with _constants=c })
  let cur_cli_req = (fun s -> s._cur_cli_req), (fun s c -> { s with _cur_cli_req=c })
  let valid_inputs = (fun s -> s._valid_inputs), (fun s v -> { s with _valid_inputs=v })

  let compare t n i =
    let c_diff = match t._round with
      | n' when n > n' -> N_BEHIND
      | n' when n < n' -> N_AHEAD
      | n' -> N_EQUAL
    in
    let p_diff = match Int64.succ t._proposed with
      | i' when i > i' -> P_BEHIND
      | i' when i < t._proposed -> P_AHEAD
      | i' ->
          if t._proposed = t._accepted
          then
            if i = Int64.succ t._proposed
            then P_ACCEPTABLE
            else P_AHEAD
          else P_ACCEPTABLE
    in
    let a_diff = match Int64.succ t._accepted with
      | i' when i > i' -> A_BEHIND
      | i' when i < i' -> A_AHEAD
      | i' -> A_COMMITABLE
    in
    (c_diff, p_diff, a_diff)
end

module rec Action : sig
  type t = A_RESYNC of node_id * tick * tick
         | A_SEND_MSG of Message.t * node_id
         | A_BROADCAST_MSG of Message.t
         | A_COMMIT_UPDATE of tick * update * request_awakener option
         | A_LOG_UPDATE of tick * update * request_awakener option
         | A_START_TIMER of tick * tick * float
         | A_CLIENT_REPLY of client_reply
         | A_STORE_LEASE of node_id option

  val to_string : t -> string
end = struct
  type t = A_RESYNC of node_id * tick * tick
         | A_SEND_MSG of Message.t * node_id
         | A_BROADCAST_MSG of Message.t
         | A_COMMIT_UPDATE of tick * update * request_awakener option
         | A_LOG_UPDATE of tick * update * request_awakener option
         | A_START_TIMER of tick * tick * float
         | A_CLIENT_REPLY of client_reply
         | A_STORE_LEASE of node_id option

  let to_string = function
    | A_RESYNC (node_id, t1, t2) ->
        Printf.sprintf "A_RESYNC (\"%s\", %s, %s)"
          node_id (tick2s t1) (tick2s t2)
    | A_SEND_MSG (msg, node_id) ->
        Printf.sprintf "A_SEND_MSSG (%s, \"%s\")"
          (Message.to_string msg) node_id
    | A_BROADCAST_MSG msg ->
        Printf.sprintf "A_BROADCAST_MSG (%s)"
          (Message.to_string msg)
    | A_COMMIT_UPDATE (t, u, r) ->
        Printf.sprintf "A_COMMIT_UPDATE (%s, %s, %s)"
          (tick2s t) (string_of_update u)
          (string_of_option (const "<request_awakener>") r)
    | A_LOG_UPDATE (t, u, r) ->
        Printf.sprintf "A_COMMIT_UPDATE (%s, %s, %s)"
          (tick2s t) (string_of_update u)
          (string_of_option (const "<request_awakener>") r)
    | A_START_TIMER (t1, t2, f) ->
        Printf.sprintf "A_START_TIMER (%s, %s, %f)"
          (tick2s t1) (tick2s t2) f
    | A_CLIENT_REPLY r ->
        Printf.sprintf "A_CLIENT_REPLY (%s)"
          (string_of_pair (const "<request_awakener>") result2s r)
    | A_STORE_LEASE n ->
        Printf.sprintf "A_STORE_LEASE (%s)"
          (string_of_option string_of_string n)
end
and StepResult : sig
  type t = Success of (Action.t list * State.t)
         | Failure of string

  val to_string : t -> string
end = struct
  type t = Success of (Action.t list * State.t)
         | Failure of string

  let to_string = function
    | Success (actions, state) ->
        Printf.sprintf "Success (%s, %s)"
          (string_of_list Action.to_string actions)
          (State.to_string state)
    | Failure s -> "Failure " ^ string_of_string s
end

and Message : sig
  type t
  val to_string : t -> string
  val encode : t -> string
  val decode : string -> t
end = struct

  module type MESSAGE = sig
    type t

    val to_string : t -> string

    (*val handle : State.t -> t -> StepResult.t*)
  end

  module type SERIALIZABLE_MESSAGE = sig
    include MESSAGE

    val encode : t -> Buffer.t -> unit
    val decode : string -> int -> (t * int)
  end

  module rec MPrepare : sig
    include SERIALIZABLE_MESSAGE
    val make : node_id -> NTick.t -> MTick.t -> ITick.t -> t
  end = struct
    type t = { src : node_id
             ; n : NTick.t
             ; m : MTick.t
             ; i : ITick.t
             }

    let to_string t =
      Printf.sprintf "MPrepare.t { src=\"%s\"; n=%s; m=%s; i=%s }"
        t.src (NTick.to_string t.n) (MTick.to_string t.m) (ITick.to_string t.i)


    let encode t buf =
      Llio.string_to buf t.src;
      NTick.encode buf t.n;
      MTick.encode buf t.m;
      ITick.encode buf t.i

    let decode s off =
      let (src, off) = Llio.string_from s off in
      let (n, off) = NTick.decode s off in
      let (m, off) = MTick.decode s off in
      let (i, off) = ITick.decode s off in
      ({ src=src; n=n; m=m; i=i }, off)

    let make src n m i =
      { src=src; n=n; m=m; i=i }
  end

  and MPromise : SERIALIZABLE_MESSAGE = struct
    type t = { src : node_id
             ; n : NTick.t
             ; m : MTick.t
             ; i : ITick.t
             ; update : update option
             }

    let to_string t =
      Printf.sprintf "MPromise.t { src=\"%s\"; n=%s; m=%s; i=%s; update=%s }"
        t.src (NTick.to_string t.n) (MTick.to_string t.m)
        (ITick.to_string t.i) (string_of_option string_of_update t.update)

    let encode t buf =
      Llio.string_to buf t.src;
      NTick.encode buf t.n;
      MTick.encode buf t.m;
      ITick.encode buf t.i;

      match t.update with
        | None ->
            Llio.bool_to buf false
        | Some upd ->
            Llio.bool_to buf true;
            update_to buf upd

    let decode s off =
      let (src, off) = Llio.string_from s off in
      let (n, off) = NTick.decode s off in
      let (m, off) = MTick.decode s off in
      let (i, off) = ITick.decode s off in
      let (update, off) = match Llio.bool_from s off with
        | (false, off) -> (None, off)
        | (true, off) ->
            let (upd, off) = update_from s off in
            (Some upd, off)
      in
      ({ src=src; n=n; m=m; i=i; update=update }, off)
  end

  and MNack : SERIALIZABLE_MESSAGE = struct
    type t = { src : node_id
             ; n : NTick.t
             ; m : MTick.t
             ; i : ITick.t
             }

    let to_string t =
      Printf.sprintf "MNack.t { src=\"%s\"; n=%s; m=%s; i=%s }"
        t.src (NTick.to_string t.n) (MTick.to_string t.m) (ITick.to_string t.i)

    let encode t buf =
      Llio.string_to buf t.src;
      NTick.encode buf t.n;
      MTick.encode buf t.m;
      ITick.encode buf t.i

    let decode s off =
      let (src, off) = Llio.string_from s off in
      let (n, off) = NTick.decode s off in
      let (m, off) = MTick.decode s off in
      let (i, off) = ITick.decode s off in
      ({ src=src; n=n; m=m; i=i }, off)
  end

  and MAccept : SERIALIZABLE_MESSAGE = struct
    type t = { src : node_id
             ; n : NTick.t
             ; m : MTick.t
             ; i : ITick.t
             ; update : update
             }

    let to_string t =
      Printf.sprintf "MAccept.t { src=\"%s\"; n=%s; m=%s; i=%s; update=%s }"
        t.src (NTick.to_string t.n) (MTick.to_string t.m) (ITick.to_string t.i)
        (string_of_update t.update)

    let encode t buf =
      Llio.string_to buf t.src;
      NTick.encode buf t.n;
      MTick.encode buf t.m;
      ITick.encode buf t.i;
      update_to buf t.update

    let decode s off =
      let (src, off) = Llio.string_from s off in
      let (n, off) = NTick.decode s off in
      let (m, off) = MTick.decode s off in
      let (i, off) = ITick.decode s off in
      let (update, off) = update_from s off in
      ({ src=src; n=n; m=m; i=i; update=update }, off)
  end

  and MAccepted : SERIALIZABLE_MESSAGE = struct
    type t = { src : node_id
             ; n : NTick.t
             ; m : MTick.t
             ; i : ITick.t
             }

    let to_string t =
      Printf.sprintf "MAccepted.t { src=\"%s\"; n=%s; m=%s; i=%s }"
      t.src (NTick.to_string t.n) (MTick.to_string t.m) (ITick.to_string t.i)

    let encode t buf =
      Llio.string_to buf t.src;
      NTick.encode buf t.n;
      MTick.encode buf t.m;
      ITick.encode buf t.i

    let decode s off =
      let (src, off) = Llio.string_from s off in
      let (n, off) = NTick.decode s off in
      let (m, off) = MTick.decode s off in
      let (i, off) = ITick.decode s off in
      ({ src=src; n=n; m=m; i=i }, off)
  end

  and MLeaseTimeout : MESSAGE = struct
    type t = { n : NTick.t
             ; m : MTick.t
             }

    let to_string t =
      Printf.sprintf "MLeaseTimeout.t { n=%s; m=%s }"
      (NTick.to_string t.n) (MTick.to_string t.m)

    let start_elections (new_n : NTick.t) (m : MTick.t) state =
      let open State.StateName in
      let me = read (State.PaxosConstants.me <.> State.constants) state in

      let sn, mid = match read State.state_name state with
        | S_MASTER ->
            if read State.round state = NTick.to_int64 new_n
            then (S_MASTER, Some me)
            else (S_RUNNING_FOR_MASTER, None)
        | _ ->
            (S_RUNNING_FOR_MASTER, None)
      in

      let msg = Sum.M_PREPARE (MPrepare.make me new_n m (ITick.succ (read State.accepted state))) in
      let new_state = multi_update state [
        (State.round, new_n);
        (State.extensions, m);
        (State.master_id, mid);
        (State.state_name, sn);
        (State.votes, []);
        (State.election_votes, State.ElectionVotes.empty)
      ] in
      let lease_duration = read (State.PaxosConstants.lease_duration <.> State.constants) state in
      let lease_expiry = build_lease_timer new_n m (lease_duration /. 2.0) in
      Success ([A_STORE_LEASE mid; A_BROADCAST_MSG msg; lease_expiry], new_state)

    let handle state t =
      match State.compare state t.n 0L with
        | (N_EQUAL, _, _) when t.m = read State.extensions state -> begin
            match read State.master_id state with
              | Some m when m = read (State.PaxosConstants.me <.> State.constants) state ->
                  let new_m = Int64.succ (read State.extensions state) in
                  start_elections (read State.round state) new_m state
              | _ ->
                  let node_cnt = read (State.PaxosConstants.node_cnt <.> State.constants) state
                  and node_ix = read (State.PaxosConstants.node_ix <.> State.constants) state in
                  let new_n = next_n t.n node_cnt node_ix in
                  start_elections new_n 0L state
          end
        | (_, _, _) -> Success([], state)
  end

  and MClientRequest : MESSAGE = struct
    type t = { request_awakener : request_awakener
             ; update : update
             }

    let to_string t =
      Printf.sprintf "MClientRequest.t { request_awakener; update=%s }"
      (string_of_update (t.update))

    let handle state t =
      let open State.StateName in
      let open Action in
      let open StepResult in
      match read State.state_name state with
        | S_MASTER ->
            match read State.cur_cli_req state with
              | None ->
                  let next_i = Int64.succ (read State.accepted state) in
                  let action = A_LOG_UPDATE(TICK next_i, t.update, Some t.request_awakener) in
                  Success([action], state)
              | Some _ -> (* This diverges from the original code, since I think it's wrong due to aliasing of 'w' *)
                  let exc = FAILURE(Arakoon_exc.E_UNKNOWN_FAILURE, "Cannot process request, already handling a client request") in
                  let reply = (t.request_awakener, exc) in
                  Success([A_CLIENT_REPLY reply], state)
        | _ ->
            let exc = FAILURE(Arakoon_exc.E_NOT_MASTER, "Cannot process request on non-master") in
            let reply = (t.request_awakener, exc) in
            Success([A_CLIENT_REPLY reply], state)
  end

  and MMasterSet : SERIALIZABLE_MESSAGE = struct
    type t = { master_id : node_id
             ; n : NTick.t
             ; m : MTick.t
             }

    let to_string t =
      Printf.sprintf "MMasterSet.t { master_id=\"%s\"; n=%s; m=%s }"
      t.master_id (NTick.to_string t.n) (MTick.to_string t.m)

    let encode t buf =
      Llio.string_to buf t.master_id;
      NTick.encode buf t.n;
      MTick.encode buf t.m

    let decode s off =
      let (master_id, off) = Llio.string_from s off in
      let (n, off) = NTick.decode s off in
      let (m, off) = MTick.decode s off in
      ({ master_id=master_id; n=n; m=m }, off)

    let handle state t =
      let open State.StateName in
      let open Action in
      let open StepResult in
      match read State.state_name state with
        | S_SLAVE when read State.round state = t.n ->
            let l = A_STORE_LEASE (Some t.master_id) in
            let t =
              if read State.round state = t.n && read State.extensions state = t.m
              then []
              else
                let d = read (State.PaxosConstants.lease_duration <.> State.constants) state in
                [A_START_TIMER (TICK t.n, TICK t.m, d)]
            in
            Success (l :: t, state)
        | _ ->
            Success ([], state)
  end

  and Sum : sig
    type t = M_PREPARE of MPrepare.t
           | M_PROMISE of MPromise.t
           | M_NACK of MNack.t
           | M_ACCEPT of MAccept.t
           | M_ACCEPTED of MAccepted.t
           | M_LEASE_TIMEOUT of MLeaseTimeout.t
           | M_CLIENT_REQUEST of MClientRequest.t
           | M_MASTERSET of MMasterSet.t
  end = struct
    type t = M_PREPARE of MPrepare.t
           | M_PROMISE of MPromise.t
           | M_NACK of MNack.t
           | M_ACCEPT of MAccept.t
           | M_ACCEPTED of MAccepted.t
           | M_LEASE_TIMEOUT of MLeaseTimeout.t
           | M_CLIENT_REQUEST of MClientRequest.t
           | M_MASTERSET of MMasterSet.t
  end

  type t = Sum.t

  let to_string = function
    | M_PREPARE v -> MPrepare.to_string v
    | M_PROMISE v -> MPromise.to_string v
    | M_NACK v -> MNack.to_string v
    | M_ACCEPT v -> MAccept.to_string v
    | M_ACCEPTED v -> MAccepted.to_string v
    | M_LEASE_TIMEOUT v -> MLeaseTimeout.to_string v
    | M_CLIENT_REQUEST v -> MClientRequest.to_string v
    | M_MASTERSET v -> MMasterSet.to_string v

  let encode t =
    let (id, f) = match t with
      | M_PREPARE v -> (1, MPrepare.encode v)
      | M_PROMISE v -> (2, MPromise.encode v)
      | M_NACK v -> (3, MNack.encode v)
      | M_ACCEPT v -> (4, MAccept.encode v)
      | M_ACCEPTED v -> (5, MAccepted.encode v)
      | M_MASTERSET v -> (6, MMasterSet.encode v)
      | M_LEASE_TIMEOUT _ ->
          failwith "Lease timeout message cannot be serialized"
      | M_CLIENT_REQUEST _ ->
          failwith "Client request message cannot be serialized"
    in
    let buf = Buffer.create 8 in
    Llio.int_to buf id;
    f buf;
    Buffer.contents buf

  let decode s =
    let (kind, off) = Llio.int_from s 0 in

    match kind with
      | 1 -> M_PREPARE (fst (MPrepare.decode s off))
      | 2 -> M_PROMISE (fst (MPromise.decode s off))
      | 3 -> M_NACK (fst (MNack.decode s off))
      | 4 -> M_ACCEPT (fst (MAccept.decode s off))
      | 5 -> M_ACCEPTED (fst (MAccepted.decode s off))
      | 6 -> M_MASTERSET (fst (MMasterSet.decode s off))
      | i -> failwith (Printf.sprintf "Unknown message type %d" i)
end

module MULTI = struct

  let build_mp_constants q n n_others lease ix node_cnt =
    {
      quorum = q;
      me = n;
      others = n_others;
      lease_duration = lease;
      node_ix = ix;
      node_cnt = node_cnt;
    } 
  
  let ch_all              = [CH_CLIENT; CH_NODE; CH_TIMEOUT]
  let ch_node_and_timeout = [CH_NODE; CH_TIMEOUT] 
  
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

  
  
  let client_reply2s = function
    | w, FAILURE(rc,msg) -> 
      Printf.sprintf "Reply (rc: %d) (msg: '%s')" (Arakoon_exc.int_of_rc rc) msg
    | w, VOID -> "Reply success (void)"
    | w, VALUE v -> Printf.sprintf "Reply success (value)" 
       

  
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
  val dispatch : t -> State.t -> Action.t -> State.t Lwt.t
end
