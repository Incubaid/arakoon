
type id = Mp.MULTI.node_id
let id2s i = i

type kind = Mp.MULTI.message
let kind2s m = Mp.MULTI.msg2s m

type n = Core.NTick.t
let n2s = Core.NTickUtils.tick2s

type message = {
  s: id; (* source *)
  t: id; (* target *)
  n: n;
  k: kind
}
let msg2s m= Printf.sprintf "(%s,%s,%s,%s)" (n2s m.n) (id2s m.s) (id2s m.t) (kind2s m.k)
let create_message s t n k = {s; t; n; k}

type value = Core.update
module MemStoreDispatcher = Dispatcher.ADispatcher(Mem_store.MemStore)
type agent = {
  state : Mp.MULTI.state;
  id : id;
  pop : id list;
  store : Mem_store.MemStore.t;
}
type network = message list
type agents = agent list

type state = {
  net: network;
  ags: agents
}

type move =
  | DeliverMsg of message
  | Wipe of id
let move2s = function
  | DeliverMsg msg -> Printf.sprintf "Deliver %s" (msg2s msg)
  | Wipe id        -> Printf.sprintf "Wipe %s" (id2s id)
let move2l = function
  | DeliverMsg m ->
    Printf.sprintf "%s->%s:%s;(n=%s)"
      (id2s m.s) (id2s m.t) (kind2s m.k) (n2s m.n)
  | Wipe id           -> Printf.sprintf "wipe %s" (id2s id)


let find_agent agents sid = List.find (fun a -> a.id = sid) agents
let replace_agent agent agents = List.map (fun x -> if x.id = agent.id then agent else x) agents


let generate_moves state =
  let deliver = List.map (fun x -> (DeliverMsg x:move)) state.net
  and wipe = List.map (fun a -> Wipe a.id) state.ags in
  wipe @ deliver


let constants = Mp.MULTI.build_mp_constants 2 "node_id" ["node0"; "node1"; "node2"] 8.0 1 3
let proposed = Core.ITick.from_int64 1L
let accepted = Core.ITick.from_int64 0L
let u = None (* core.update option *)


let state_label state =
  let as2l a = match a.Mp.MULTI.state_n with
    | Mp.MULTI.S_RUNNING_FOR_MASTER -> "R"
    | Mp.MULTI.S_CLUELESS -> "C"
    | Mp.MULTI.S_SLAVE -> "S"
    | Mp.MULTI.S_MASTER -> "M" in
  List.fold_left (fun acc a -> acc ^ (Printf.sprintf "%s%s%s" (as2l a.state) (Core.ITickUtils.tick2s a.state.Mp.MULTI.proposed) (Core.ITickUtils.tick2s a.state.Mp.MULTI.accepted))) "" state.ags

let n = Core.NTick.from_int64 0L

let make_agent id pop =
  let range n =
    let rec range_inner = function
      | 0 -> []
      | i -> (n-i) :: range_inner (i-1)
    in
    range_inner n in
  let nodes_with_ixs = List.combine pop (range (List.length pop)) in
  let my_ix = List.assoc id nodes_with_ixs in
  let store = Lwt_main.run (Mem_store.MemStore.create "mystore?" false) in
  {
    id;
    pop;
    state = Mp.MULTI.build_state {constants with Mp.MULTI.me = id; Mp.MULTI.node_ix = my_ix} n proposed accepted u;
    store = store;
  }

module type ALGO = sig
  val handle_message : agent -> message -> agent * message list
end
module type VALUE = sig
  type t
end

module Simulator(A:ALGO)(V:VALUE) = struct
  type value = V.t
  type path = (move * state) list

  exception Bad of (path * state * string)

  module TSet = Set.Make(struct
    type t = (string * move * string)
    let compare = Pervasives.compare
  end)

  let remove x net  = List.filter (fun m -> m <> x) net

  let execute_move move state path observed =

    let deliver m ag =
      try
        let agent = find_agent ag m.t in
        let agent', extra = A.handle_message agent m in
        let ag' = replace_agent agent' ag in
        ag', extra
      with Failure msg ->
        print_endline m.t;
        print_endline (move2s move);
        raise (Bad (path, state, msg))
    in

    let state' =
      match move with
        | DeliverMsg m ->
          let net' = remove m state.net in
          let ags',extra = deliver m state.ags in
          { net = net' @ extra; ags = ags'}
        | Wipe id ->
          let agent = find_agent state.ags id in
          let clean = make_agent id agent.pop in
          let ags' = replace_agent clean state.ags in
          {state with ags = ags'}
    in
    let observed' =
      let transition = (state_label state, move, state_label state') in
      TSet.add transition observed in
    state' , (move,state) :: path , observed'

  let dump_path path =
    let rp = List.rev path in
    List.iter
      (fun (move,state) ->
        match move with
          | DeliverMsg m -> Printf.eprintf "%s ---(%s:%8s) ---> %s\n"
            (id2s m.s) (n2s m.n) (kind2s m.k) (id2s m.t)
          | _ -> Printf.eprintf "%s\n" (move2s move)
      ) rp

  let is_consistent state =
    (*
what is consistent?
- same values in store (not applicable here?)
- only 1 master
 *)
    let masters = List.filter (fun ag -> ag.state.Mp.MULTI.state_n = Mp.MULTI.S_MASTER) state.ags in
    1 >= List.length masters

  let limit = 10

  let rec check_all level state path observed =
    if not (is_consistent state) then raise (Bad (path,state, "not consistent"));
    if level = limit
    then observed
    else
      let rec loop observed = function
        | [] -> observed
        | move :: rest ->
            let state', path', observed' = execute_move move state path observed in
            let observed'' = check_all (level + 1) state' path' observed' in
            loop observed'' rest
      in
      let moves = generate_moves state in
      loop observed moves

  let run state0 = check_all 0 state0 [] TSet.empty

  let dottify oc state0 observed =
    Printf.fprintf oc "digraph \"basic paxos\" {\n\trankdir=LR\n";
    Printf.fprintf oc "\tnode [shape = \"box\"]\n";
    Printf.fprintf oc "\t%s\n" (state_label state0);
    TSet.iter (fun (l0,m,l1) ->
      let ms, extra = match m with
        | Wipe _ ->
          (move2s m),
          "fontcolor=red color=red constraint=false"
        | DeliverMsg m   -> Printf.sprintf"{%s;%s->%s;n=%s}" (kind2s m.k)
          (id2s m.s) (id2s m.t) (n2s m.n), ""
      in
      if l0 <> l1 then
        let s = Printf.sprintf "\t%s -> %s [label=\"%s\" %s]\n" l0 l1 ms extra in
        output_string oc s)
      observed;
    Printf.fprintf oc "}\n"
end

let others agent = List.filter ((<>) agent.id) agent.pop



module Fsm = struct
  let handle_message a m : agent * message list =
    match Mp.MULTI.step m.k a.state with
      | Mp.MULTI.StepFailure msg -> failwith msg
      | Mp.MULTI.StepSuccess (actions, s') ->
          let dispatch_action =
            let get_n = function
              | Mp.MULTI.M_PREPARE (_, n, _, _) | Mp.MULTI.M_PROMISE (_, n, _, _, _)
              | Mp.MULTI.M_NACK (_, n, _, _) | Mp.MULTI.M_ACCEPT (_, n, _, _, _)
              | Mp.MULTI.M_MASTERSET (_, n, _) -> n in
            function
              | Mp.MULTI.A_SEND_MSG (msg, node) ->
                  [create_message a.id node (get_n msg) msg]
              | Mp.MULTI.A_START_TIMER (n, msg, d) ->
                  [create_message a.id a.id n (Mp.MULTI.M_LEASE_TIMEOUT (n, msg))]
              | Mp.MULTI.A_BROADCAST_MSG (msg) ->
                  List.map (fun o -> create_message a.id o (get_n msg) msg) (others a)
              | Mp.MULTI.A_STORE_LEASE (mido) ->
                  (* what's up with this? why does the store track who is master?? *)
                  let buf = Buffer.create 32 in
                  Llio.string_option_to buf mido;
                  let s = Buffer.contents buf in
                  let lwtt = Mem_store.MemStore.set_meta a.store s in
                  let () = Lwt_main.run lwtt in
                  []
          in
          ({a with state = s'}, List.concat (List.map dispatch_action actions))
end

module M = Simulator(Fsm)(struct type t = value end)

let start agent value =
  match agent.state.Mp.MULTI.state_n with
    | Mp.MULTI.S_CLUELESS ->
        let me = agent.id in
        let n' = Core.NTickUtils.next_tick agent.state.Mp.MULTI.round  in
        let targets = others agent in
        let mtick = Core.MTick.from_int64 0L in
        let msgs = List.map (fun a -> create_message me a n' (Mp.MULTI.M_PREPARE (me, n, mtick, proposed))) targets in
        ({agent with state = {agent.state with Mp.MULTI.state_n = Mp.MULTI.S_RUNNING_FOR_MASTER}}, msgs)
    | _ -> failwith "can't start here"


(*
  time ./barakoon.native --caulking | tee caulking
   dot -O -Tsvg caulking
*)
let run () =
  let ids = List.map (fun i -> "node" ^ (string_of_int i)) [0;1;2] in
  let a1,a1_out = start (make_agent "node0" ids) "x" in
  let a2,a2_out = (make_agent "node1" ids),[] in
  let a3,a3_out = (make_agent "node2" ids),[] in
  let world = [a1;a2;a3] in
  let state0 = {net = a1_out @ a2_out @ a3_out; ags = world} in
  try
    let observed = M.run state0 in
    M.dottify (Unix.out_channel_of_descr Unix.stdout) state0 observed
  with (M.Bad (path, state, m)) ->
    Printf.eprintf "bad path:\n";
    M.dump_path path;
    Printf.eprintf "%s\n" (state_label state);
    Printf.eprintf "message: %s" m


