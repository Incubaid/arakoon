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

open Multi_paxos_type
open Master_type
open Multi_paxos
open Lwt
open Mp_msg.MPMessage
open Update

let is_empty = function
  | [] -> true
  | _ -> false

(* a (possibly potential) master has found consensus on a value
   first potentially finish of a client request and then on to
   being a stable master *)
let master_consensus (type s) constants ((finished_funs : master_option),v,n,i, lease_expire_waiters) () =
  let con_e = EConsensus(finished_funs, v,n,i) in
  let log_e = ELog (fun () ->
    Printf.sprintf "on_consensus for : %s => %i finished_fs (in master_consensus)"
      (Value.value2s v) (List.length finished_funs) )
  in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  match (Value.is_master_set v, is_empty lease_expire_waiters, S.who_master constants.store) with
  | false, false, None ->
    (* there is no master, this ain't a masterset, and there is no drop master going on
       so let's push MasterSet for myself *)
    let mv =Value.create_master_value (constants.me, 0L) in
    let i' = Sn.succ i in
    push_value constants mv n i' >>= fun () ->
    let nnodes = List.length constants.others + 1 in
    let needed = constants.quorum_function nnodes in
    let new_ballot = (needed-1 , [constants.me] ) in
    Fsm.return
      ~sides:[con_e;log_e]
      (Accepteds_check_done ([(fun _ -> Lwt.return ())], n, i', new_ballot, mv, []))
  | _ ->
    begin
      let inject_e = EGen (fun () ->
        match v with
        | Value.Vm _ ->
          let event = Multi_paxos.FromClient [(Update.Nop, fun _ -> Lwt.return ())] in
          Lwt.ignore_result (constants.inject_event event);
          Lwt.return ()
        | _ ->
          Lwt.return ()
      )
      in
      let state = (v,n,(Sn.succ i), lease_expire_waiters) in
      Fsm.return ~sides:[con_e;log_e;inject_e] (Stable_master state)
    end

let stable_master (type s) constants ((v',n,new_i, lease_expire_waiters) as current_state) ev = 
  match ev with
  | LeaseExpired n' ->
      let me = constants.me in
      if n' < n 
      then
	    begin
          let log_e = ELog (fun () ->
	        Printf.sprintf "stable_master: ignoring old lease_expired with n:%s < n:%s" 
	          (Sn.string_of n') (Sn.string_of n))
          in
	      Fsm.return ~sides:[log_e] (Stable_master current_state)
	    end
      else
	    begin
	      let extend () =
                if not (is_empty lease_expire_waiters)
                then
                  let log_e = ELog (fun () ->
                    "stable_master: half-lease_expired, but not renewing lease")
                  in
                  (* TODO Is this correct, to initiate handover? *)
                  Fsm.return ~sides:[log_e] (Stable_master current_state)
                else
                  let log_e = ELog (fun () -> "stable_master: half-lease_expired: update lease." ) in
                  let v = Value.create_master_value (me,0L) in
                  let ff = fun _ -> Lwt.return () in
		    (* TODO: we need election timeout as well here *)
                  Fsm.return ~sides:[log_e] (Master_dictate ([ff], v,n,new_i, lease_expire_waiters))
              in
	      match constants.master with
	        | Preferred ps when not (List.mem me ps) ->
                  let lws = List.map (fun name -> (name, constants.last_witnessed name)) ps in
                  (* Multiply with -1 to get a reverse-sorted list *)
                  let slws = List.fast_sort (fun (_, a) (_, b) -> (-1) * compare a b) lws in
                  let (p, p_i) = List.hd slws in
	          let diff = Sn.diff new_i p_i in
	          if diff < (Sn.of_int 5) 
              then
		        begin
                  let log_e = ELog (fun () -> Printf.sprintf "stable_master: handover to %s" p) in
		          Fsm.return ~sides:[log_e] (Stable_master current_state)
		        end
	          else
		        extend () 
	        | _ -> extend()
	    end
    | FromClient ufs ->
        begin
          let updates, finished_funs = List.split ufs in
          let synced = List.fold_left (fun acc u -> acc || Update.is_synced u) false updates in
          let value = Value.create_client_value updates synced in
	      Fsm.return (Master_dictate (finished_funs, value, n, new_i, lease_expire_waiters))
        end
    | FromNode (msg,source) ->
      begin
	    let me = constants.me in
	    match msg with
	      | Prepare (n',i') ->
	        begin
              if am_forced_master constants me
	          then
		        begin
		          let reply = Nak(n', (n,new_i)) in
		          constants.send reply me source >>= fun () ->
		          if n' > 0L 
		          then
		            let new_n = update_n constants n' in
		            Fsm.return (Forced_master_suggest (new_n,new_i))
		          else
		            Fsm.return (Stable_master current_state )
		        end
	          else
		        begin
                  let module S = (val constants.store_module : Store.STORE with type t = s) in
		          handle_prepare constants source n n' i' >>= function
		            | Nak_sent 
		            | Prepare_dropped -> Fsm.return  (Stable_master current_state )
		            | Promise_sent_up2date ->
		              begin
                        let l_val = constants.tlog_coll # get_last () in
                        Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
			            Fsm.return (Slave_wait_for_accept (n', new_i, None, l_val))
		              end
		            | Promise_sent_needs_catchup ->
                      let i = S.get_catchup_start_i constants.store in
                      Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
                      Fsm.return (Slave_discovered_other_master (source, i, n', i'))
		        end
	        end
          | Accepted(n,i) -> 
              (* This one is not relevant anymore, but we're interested
                 to see the slower slaves in the statistics as well :
                 TODO: should not be solved on this level.
              *)
              let () = constants.on_witness source i in
              Fsm.return (Stable_master current_state)
          | Accept(n',i',v) when n' > n && i' > new_i ->
            (* 
               somehow the others decided upon a master and I got no event my lease expired.
               Let's see what's going on, and maybe go back to elections
            *)
            begin
              Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
              let run_elections, why = Slave.time_for_elections constants n (Some v') in
              let log_e = 
                ELog (fun () -> 
                  Printf.sprintf "XXXXX received Accept(n:%s,i:%s) time for elections? %b %s" 
                    (Sn.string_of n') (Sn.string_of i')
                    run_elections why)
              in
              let sides = [log_e] in
              if run_elections 
              then 
                Fsm.return ~sides (Election_suggest (n,new_i,Some v'))
              else
                Fsm.return ~sides (Stable_master (v',n,new_i, []))
            end
	      | _ ->
	          begin
                let log_e = ELog (fun () -> 
                  Printf.sprintf "stable_master received %S: dropping" (string_of msg)) 
                in
	            Fsm.return ~sides:[log_e] (Stable_master current_state)
	          end
      end
    | ElectionTimeout n' -> 
        begin
          let log_e = ELog (fun () ->
            Printf.sprintf "ignoring election timeout (%s)" (Sn.string_of n') )
          in          
          Fsm.return ~sides:[log_e] (Stable_master current_state)
      end
    | Quiesce (sleep,awake) ->
      begin
        fail_quiesce_request constants.store sleep awake Quiesced_fail_master >>= fun () ->
        Fsm.return (Stable_master current_state)
      end
        
    | Unquiesce -> Lwt.fail (Failure "Unexpected unquiesce request while running as")

    | DropMaster (sleep, awake) ->
        let state' = (v',n,new_i, (sleep, awake) :: lease_expire_waiters) in
        Fsm.return (Stable_master state')
                
(* a master informes the others of a new value by means of Accept
   messages and then waits for Accepted responses *)

let master_dictate constants (mo,v,n,i, lease_expire_waiters) () =
  let accept_e = EAccept (v,n,i) in
  
  let start_e = EStartLeaseExpiration (v,n,false) in
  let mcast_e = EMCast (Accept(n,i,v)) in
  let me = constants.me in
  let others = constants.others in
  let needed = constants.quorum_function (List.length others + 1) in
  let needed' = needed - 1 in
  let ballot = (needed' , [me] ) in
  
  let log_e = 
    ELog (fun () ->
      Printf.sprintf "master_dictate n:%s i:%s needed:%d" 
        (Sn.string_of n) (Sn.string_of i) needed' 
    )
  in
  let sides =
    if is_empty lease_expire_waiters
    then
      [accept_e;
       start_e;
       mcast_e;
       log_e;
      ]
    else
      [accept_e;
       mcast_e;
       log_e;
      ]
  in
  Fsm.return ~sides (Accepteds_check_done (mo, n, i, ballot, v, lease_expire_waiters))
