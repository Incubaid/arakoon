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

(* a (possibly potential) master has found consensus on a value
   first potentially finish of a client request and then on to
   being a stable master *)
let master_consensus constants ((finished_funs : master_option),v,n,i) () =
  constants.on_consensus (v,n,i) >>= fun (urs: Store.update_result list) ->
  begin
    let rec loop ffs urs =
      match (ffs,urs) with
        | [],[] -> Lwt.return ()
        | finished_f :: ffs , update_result :: urs -> 
            finished_f update_result >>= fun () ->
            loop ffs urs
        | _,_ -> failwith "mismatch"
    in
    Lwt_log.debug_f "on_consensus for : %s => %i finished_fs %i result(s) " 
      (Value.value2s v) (List.length finished_funs) (List.length urs) >>= fun ()->
    loop finished_funs urs
  end >>= fun () ->
  let state = (v,n,(Sn.succ i)) in
  Fsm.return (Stable_master state)
    


let stable_master constants ((v',n,new_i) as current_state) = function
  | LeaseExpired n' ->
      let me = constants.me in
      if n' < n 
      then
	    begin
	      log ~me "stable_master: ignoring old lease_expired with n:%s < n:%s" 
	        (Sn.string_of n') (Sn.string_of n) >>= fun () ->
	      Fsm.return (Stable_master current_state)
	    end
      else
	    begin
	      let extend () = 
	        log ~me "stable_master: half-lease_expired: update lease." 
	        >>= fun () ->
            let v = Value.create_master_value (me,0L) in
            let ff = fun _ -> Lwt.return () in
		    (* TODO: we need election timeout as well here *)
	        Fsm.return (Master_dictate ([ff], v,n,new_i))
	      in
	      match constants.master with
	        | Preferred p when p <> me ->
	          let diff = Sn.diff new_i (constants.last_witnessed p) in
	          if diff < (Sn.of_int 5) 
              then
		        begin
		          log ~me "stable_master: handover to %s" p >>= fun () ->
		          Fsm.return (Stable_master current_state)
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
	      Fsm.return (Master_dictate (finished_funs, value, n, new_i))
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
		          handle_prepare constants source n n' i' >>= function
		            | Nak_sent 
		            | Prepare_dropped -> Fsm.return  (Stable_master current_state )
		            | Promise_sent_up2date ->
		              begin
                        let l_val = constants.tlog_coll # get_last () in
			            Fsm.return (Slave_wait_for_accept (n', new_i, None, l_val))
		              end
		            | Promise_sent_needs_catchup ->
                      let i = Store.get_catchup_start_i constants.store in
                      Fsm.return (Slave_discovered_other_master (source, i, n', i'))
		        end
	        end
          | Accepted(n,i) -> (* This one is not relevant anymore, but we're interested
                                to see the slower slaves in the statistics as well :
                                TODO: should not be solved on this level.
                             *)
	        log ~me "stable_master received %S: dropping (but witnessing)" (string_of msg) >>= fun () ->
            let () = constants.on_witness source i in
            Fsm.return (Stable_master current_state)
	      | _ ->
	        begin
	          log ~me "stable_master received %S: dropping" (string_of msg)
	          >>= fun () ->
	          Fsm.return (Stable_master current_state)
		        
	        end
      end
    | ElectionTimeout n' -> 
      begin
      let me = constants.me in
      log ~me "ignoring election timeout (%s)" (Sn.string_of n') >>= fun () ->
      Fsm.return (Stable_master current_state)
      end
    | Quiesce (sleep,awake) ->
      begin
        fail_quiesce_request constants.store sleep awake Quiesced_fail_master >>= fun () ->
        Fsm.return (Stable_master current_state)
      end
        
    | Unquiesce -> Lwt.fail (Failure "Unexpected unquiesce request while running as")
                
(* a master informes the others of a new value by means of Accept
   messages and then waits for Accepted responses *)

let master_dictate constants (mo,v,n,i) () =
  let accept_e = EAccept (v,n,i) in
  
  let start_e = EStart (v,n) in
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
    [accept_e;
     start_e;
     mcast_e;
     log_e;
    ]
  in
  Fsm.return ~sides (Accepteds_check_done (mo, n, i, ballot, v))
