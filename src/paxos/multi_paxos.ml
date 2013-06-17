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
open Master_type


let section =
  let s = Logger.Section.make "paxos" in
  let () = Logger.Section.set_level s Logger.Debug in
  s

let quorum_function = Quorum.quorum_function

exception ConflictException of (Value.t * Value.t list)
exception TooFewPromises of (float * int * string list)

exception PaxosFatal of string
(* timeout and to_receive, and who got through *)

let paxos_fatal me fmt =
  let k x =
    Logger.fatal_ (me^": "^x) >>= fun () ->
    Lwt.fail (PaxosFatal x)
  in
  Printf.ksprintf k fmt

let can_promise (type s) (module S : Store.STORE with type t = s) store lease_expiration requester =
  match S.who_master store with
    | Some (m, ml) ->
      let l64 = Int64.of_int lease_expiration in
      if (
        ( (Int64.add ml l64) > Int64.of_float (Unix.time()) )
        &&
          (String.compare requester m) <> 0
      )
      then false
      else true
    | None -> true

type ballot = int * string list (* still needed, & who voted *)

let network_of_messaging (m:messaging) =
  (* conversion since not all code is 'networked' *)
  let send msg source target =
    Logger.debug_f_ "%s: sending msg to %s: %s" source target (Mp_msg.MPMessage.string_of msg) >>= fun () ->
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

type quiesce_result =
  | Quiesced_ok
  | Quiesced_fail_master
  | Quiesced_fail
 				      
type paxos_event =
  | FromClient of ((Update.Update.t) * (Store.update_result -> unit Lwt.t)) list
  | FromNode of (MPMessage.t * Messaging.id)
  | LeaseExpired of Sn.t
  | Quiesce of (quiesce_result Lwt.t * quiesce_result Lwt.u)
  | Unquiesce
  | ElectionTimeout of Sn.t
  | DropMaster of (unit Lwt.t * unit Lwt.u)

let paxos_event2s = function
  | FromClient _ -> "FromClient _"
  | FromNode _ -> "FromNode _ "
  | LeaseExpired _ -> "LeaseExpired _"
  | Quiesce _ -> "Quiesce _"
  | Unquiesce -> "Unquiesce _"
  | ElectionTimeout _ -> "ElectionTimeout _"
  | DropMaster _ -> "DropMaster _"

type 'a constants =
    {me:id;
     others: id list;
     send: MPMessage.t -> id -> id -> unit Lwt.t;
     get_value: Sn.t -> Value.t option;
     on_accept: Value.t * Sn.t * Sn.t -> unit Lwt.t;
     on_consensus:
       Value.t * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n ->
       (Store.update_result list) Lwt.t;
     on_witness: id -> Sn.t -> unit;
     last_witnessed: id -> Sn.t;
     quorum_function: int -> int;
     master: master;
     store: 'a;
     store_module: (module Store.STORE with type t = 'a);
     tlog_coll:Tlogcollection.tlog_collection;
     other_cfgs:Node_cfg.Node_cfg.t list;
     lease_expiration: int;
     inject_event: paxos_event -> unit Lwt.t;
     cluster_id : string;
     is_learner: bool;
     quiesced : bool;
    }

let am_forced_master constants me =
  match constants.master with
    | Forced x -> x = me
    | _ -> false

let is_election constants =
  match constants.master with
    | Elected | Preferred _ -> true
    | _ -> false

let make (type s) me is_learner others send receive get_value 
    on_accept on_consensus on_witness 
    last_witnessed quorum_function (master:master) (module S : Store.STORE with type t = s) store tlog_coll 
    other_cfgs lease_expiration inject_event ~cluster_id 
    quiesced =
  {
    me=me;
    is_learner = is_learner;
    others=others;
    send = send;
    get_value= get_value;
    on_accept = on_accept;
    on_consensus = on_consensus;
    on_witness = on_witness;
    last_witnessed = last_witnessed;
    quorum_function = quorum_function;
    master = master;
    store = store;
    store_module = (module S);
    tlog_coll = tlog_coll;
    other_cfgs = other_cfgs;
    lease_expiration = lease_expiration;
    inject_event = inject_event;
    cluster_id = cluster_id;
    quiesced = quiesced;
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
  let t () =
    begin
      Logger.debug_f_ "%s: waiting %2.1f seconds for lease to expire"
        constants.me sleep_sec >>= fun () ->
      Lwt_unix.sleep sleep_sec >>= fun () ->
      Logger.debug_f_ "%s: lease expired (%2.1f passed)=> injecting LeaseExpired event for %s"
        constants.me sleep_sec (Sn.string_of n) >>= fun () ->
      constants.inject_event (LeaseExpired n)
    end in
  let () = Lwt_extra.ignore_result (t ()) in
  Lwt.return ()

let start_election_timeout constants n =
  let sleep_sec = float_of_int (constants.lease_expiration) /. 2.0 in
  let t () = 
    begin
      Logger.debug_f_ "%s: waiting %2.1f seconds for election to finish" constants.me sleep_sec >>= fun () ->
      Lwt_unix.sleep sleep_sec >>= fun () ->
      Logger.debug_f_ "%s: election (n=%s) should have finished by now." constants.me (Sn.string_of n) >>= fun () ->
      constants.inject_event (ElectionTimeout n)
    end
  in
  let () = Lwt_extra.ignore_result (t ()) in
  Lwt.return ()

type prepare_repsonse =
  | Prepare_dropped
  | Promise_sent_up2date
  | Promise_sent_needs_catchup
  | Nak_sent

let handle_prepare (type s) constants dest n n' i' =
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  let me = constants.me in
  let () = constants.on_witness dest i' in 
  if not ( List.mem dest constants.others) then
    begin
      let store = constants.store in
      let s_i = S.consensus_i store in
      let nak_i = 
        begin
          match s_i with
            | None -> Sn.start
            | Some si -> Sn.succ si
	end in
      let reply = Nak( n',(n,nak_i)) in
      Logger.debug_f_ "%s: replying with %S to learner %s" me (string_of reply) dest
      >>= fun () ->
      constants.send reply me dest >>= fun () ->
      Lwt.return Nak_sent 
    end
  else
    begin
      let can_pr = can_promise constants.store_module constants.store constants.lease_expiration dest in
      if not can_pr && n' >= 0L
      then
	    begin 
          Logger.debug_f_ "%s: handle_prepare: Dropping prepare - lease still active" me 
	      >>= fun () ->
	      Lwt.return Prepare_dropped
	        
	    end
      else 
	    begin
          let store = constants.store in
          let s_i = S.consensus_i store in
          let nak_max = 
            begin
              match s_i with
		        | None -> Sn.start
		        | Some si -> Sn.succ si
	        end 
          in

          if ( n' > n && i' < nak_max && nak_max <> Sn.start ) || n' <= n 
          then
            (* Send Nak, other node is behind *)
            let reply = Nak( n',(n,nak_max)) in
            Logger.debug_f_ "%s: NAK:other node is behind: i':%s nak_max:%s" me 
              (Sn.string_of i') (Sn.string_of nak_max) >>= fun () ->
            Lwt.return (Nak_sent, reply) 
          else
            begin
              (* We will send a Promise, start election timer *)
              let lv = constants.get_value nak_max in
              let reply = Promise(n',nak_max,lv) in
              Logger.debug_f_ "%s: handle_prepare: starting election timer" me >>= fun () ->
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
      Logger.debug_f_ "%s: handle_prepare replying with %S" me (string_of reply) >>= fun () ->
      constants.send reply me dest >>= fun () ->
      Lwt.return ret_val
    end
      
let safe_wakeup sleeper awake value =
  Lwt.catch 
  ( fun () -> Lwt.return (Lwt.wakeup awake value) )
  ( fun e -> 
    match e with
      | Invalid_argument s ->
        let t = state sleeper in
        begin
          match t with
            | Fail ex -> Lwt.fail ex
            | Return v -> Lwt.return ()
            | Sleep -> Lwt.fail (Failure "Wakeup error, sleeper is still sleeping")
        end
      | _ -> Lwt.fail e
   ) 

let safe_wakeup_all v l =
  Lwt_list.iter_s
    (fun (s, a) -> safe_wakeup s a v)
    l

let fail_quiesce_request store sleeper awake reason =
  safe_wakeup sleeper awake reason
  
let handle_quiesce_request (type s) (module S : Store.STORE with type t = s) store sleeper (awake: quiesce_result Lwt.u) =
  S.quiesce store >>= fun () ->
  safe_wakeup sleeper awake Quiesced_ok

let handle_unquiesce_request (type s) constants n =
  let store = constants.store in
  let tlog_coll = constants.tlog_coll in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  let too_far_i = S.get_succ_store_i store in
  S.unquiesce store >>= fun () ->
  Catchup.catchup_store "handle_unquiesce" ((module S),store,tlog_coll) too_far_i >>= fun (i,vo) ->
  start_lease_expiration_thread constants n constants.lease_expiration >>= fun () ->
  Lwt.return (i,vo)
  
