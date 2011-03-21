(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
*)

open Mp_msg
open Lwt
open MPMessage
open Messaging
open Multi_paxos_type

let log ?(me="???") x =
  let k s= Lwt_log.debug (me ^ ": " ^ s) in
  Printf.ksprintf k x

let quorum_function = Quorum.quorum_function

exception ConflictException of (Value.t * Value.t list)
exception TooFewPromises of (float * int * string list)

exception PaxosFatal of string
(* timeout and to_receive, and who got through *)

let paxos_fatal me fmt =
  let k x =
    Lwt_log.fatal (me^": "^x) >>= fun () ->
    Lwt.fail (PaxosFatal x)
  in
  Printf.ksprintf k fmt

let can_promise store lease_expiration requester =
  store # who_master() >>= function
    | Some (m, ml) ->
      let l64 = Int64.of_int lease_expiration in
      if (
        ( (Int64.add ml l64) > Int64.of_float (Unix.time()) )
        &&
        (String.compare requester m) <> 0
        )
      then 
        Lwt.return false
      else 
        Lwt.return true
    | None -> Lwt.return true

type ballot = int * string list (* still needed, & who voted *)

let network_of_messaging (m:messaging) =
  (* conversion since not all code is 'networked' *)
  let send msg source target =
    let g = MPMessage.generic_of msg in
    m # send_message g ~source ~target
  in
  let receive target =
    (* log "calling receive" >>= fun () -> *)
    m # recv_message ~target >>= fun (g,s) ->
    let msg = MPMessage.of_generic g in
    Lwt.return (msg,s)
  in
  let register = m # register_receivers in
  let run () = m # run () in
  send, receive, run, register


let update_votes (nones,somes) = function
  | None -> (nones+1, somes)
  | Some x -> 
    let rec build_new acc = function
      | [] -> (x,1)::acc
      | (a,fa) :: afs -> 
	if a = x 
	then ((a,fa+1) :: afs) @ acc 
	else let acc' = (a,fa) :: acc  in build_new acc' afs
    in
    let tmp = build_new [] somes in
    let somes' = List.sort (fun (a,fa) (b,fb) -> fb - fa) tmp in
    (nones, somes')
				      
type paxos_event =
  | FromClient of ((Value.t option) * (Store.update_result -> unit Lwt.t))
  | FromNode of (MPMessage.t * Messaging.id)
  | LeaseExpired of Sn.t
  | ElectionTimeout of Sn.t

type constants =
    {me:id;
     others: id list;
     send: MPMessage.t -> id -> id -> unit Lwt.t;
     get_value: Sn.t -> Value.t option Lwt.t;
     on_accept: Value.t * Sn.t * Sn.t -> Value.t Lwt.t;
     on_consensus:
       Value.t * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n ->
       Store.update_result Lwt.t;
     on_witness: id -> Sn.t -> unit Lwt.t;
     quorum_function: int -> int;
     forced_master: string option;
     store:Store.store;
     tlog_coll:Tlogcollection.tlog_collection;
     other_cfgs:Node_cfg.Node_cfg.t list;
     lease_expiration: int;
     inject_event: paxos_event -> unit Lwt.t;
     cluster_id : string;
    }

let am_forced_master constants me =
  match constants.forced_master with
    | None -> false
    | Some x -> x = me

let is_election constants =
  match constants.forced_master with
    | None -> true
    | Some _ -> false

let make me others send receive get_value 
    on_accept on_consensus on_witness
    quorum_function forced_master store tlog_coll 
    other_cfgs lease_expiration inject_event ~cluster_id =
  {
    me=me;
    others=others;
    send = send;
    get_value= get_value;
    on_accept = on_accept;
    on_consensus = on_consensus;
    on_witness = on_witness;
    quorum_function = quorum_function;
    forced_master = forced_master;
    store = store;
    tlog_coll = tlog_coll;
    other_cfgs = other_cfgs;
    lease_expiration = lease_expiration;
    inject_event = inject_event;
    cluster_id = cluster_id;
  }

let mcast constants msg =
  let send = constants.send in
  let me = constants.me in
  let others = constants.others in
  Lwt_list.iter_p (fun o -> send msg me o) others


let update_n constants n =
  Sn.add n (Sn.of_int (1 + Random.int ( 1 + (List.length constants.others) * 2)))


let start_lease_expiration_thread constants n expiration =
  let sleep_sec = float_of_int expiration in
  let me = constants.me in
  let t () =
    begin
      log ~me "waiting %2.1f seconds for lease to expire" sleep_sec >>= fun () ->
      Lwt_unix.sleep sleep_sec >>= fun () ->
      log ~me "lease expired (%2.1f passed)=> injecting LeaseExpired event for %s" 
	sleep_sec (Sn.string_of n) >>= fun () ->
      constants.inject_event (LeaseExpired n)
    end in
  let () = Lwt.ignore_result (t ()) in
  Lwt.return ()

let start_election_timeout constants n =
  let sleep_sec = float_of_int (constants.lease_expiration) /. 2.0 in
  let me = constants.me in
  let t () = 
    begin
      log ~me "waiting %2.1f seconds for election to finish" sleep_sec >>= fun () ->
      Lwt_unix.sleep sleep_sec >>= fun () ->
      log ~me "election (n=%s) should have finished by now." (Sn.string_of n) >>= fun () ->
      constants.inject_event (ElectionTimeout n)
    end
  in
  let () = Lwt.ignore_result (t ()) in
  Lwt.return ()

type prepare_repsonse =
  | Prepare_dropped
  | Promise_sent_up2date
  | Promise_sent_needs_catchup
  | Nak_sent

let handle_prepare constants dest n n' i' =
  let me = constants.me in
  constants.on_witness dest i' >>= fun () ->
  begin
    can_promise constants.store constants.lease_expiration dest 
    >>= fun can_pr ->
    if not can_pr && n' >= 0L
    then
      begin 
        log ~me "handle_prepare: Dropping prepare - lease still active" 
	>>= fun () ->
        (* 
	   Lwt.return Prepare_dropped
	*)
	let store = constants.store in
        store # consensus_i () >>= fun s_i ->
        let nak_max = 
          begin
            match s_i with
              | None -> Sn.start
              | Some si -> Sn.succ si
	  end in
	let reply = Nak( n',(n,nak_max)) in
	log ~me "handle_prepare replying with %S" (string_of reply) 
	>>= fun () ->
	constants.send reply me dest >>= fun () ->
	Lwt.return Nak_sent
      end
    else 
      begin
        let store = constants.store in
        store # consensus_i () >>= fun s_i ->
        let nak_max = 
          begin
            match s_i with
              | None -> Sn.start
              | Some si -> Sn.succ si
	  end in
        constants.get_value(nak_max) >>= fun lv ->
        
        if ( n' > n && i' < nak_max && nak_max <> Sn.start ) || n' <= n 
        then
          (* Send Nak, other node is behind *)
          let reply = Nak( n',(n,nak_max)) in
          Lwt.return (Nak_sent, reply) 
        else
          begin
            (* We will send a Promise, start election timer *)
            constants.get_value(nak_max) >>= fun lv ->
            let reply = Promise(n',nak_max,lv) in
            log ~me "handle_prepare: starting election timer" >>= fun () ->
            start_election_timeout constants n' >>= fun () ->
            if i' > nak_max
            then
              (* Send Promise, but I need catchup *)
              Lwt.return(Promise_sent_needs_catchup, reply)
            else (* i' = i *)
              (* Send Promise, we are in sync *)
              Lwt.return(Promise_sent_up2date, reply)
          end 
      end >>= fun (ret_val, reply) ->
      log ~me "handle_prepare replying with %S" (string_of reply) >>= fun () ->
      constants.send reply me dest >>= fun () ->
      Lwt.return ret_val
  end
      
