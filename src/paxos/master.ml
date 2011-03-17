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
open Multi_paxos
open Lwt
open Mp_msg.MPMessage
open Update

(* a (possibly potential) master has found consensus on a value
   first potentially finish of a client request and then on to
   being a stable master *)
let master_consensus constants ((mo:master_option),v,n,i) () =
  constants.on_consensus (v,n,i) >>= fun so ->
  begin
    match mo with
      | Some finished ->
	(* consensus on a client request *)
	finished so
      | None -> (* first time consensus on master *)
	begin
	  let me = constants.me in
	  log ~me "running as STABLE MASTER n:%s i:%s" 
	    (Sn.string_of n) (Sn.string_of (Sn.succ i))
	end
  end >>= fun () ->
  let state = (v,n,(Sn.succ i)) in
  Lwt.return (Stable_master state)


(* an active master responsing on requests from the client *)
let stable_master constants (v',n,new_i) = function
    | LeaseExpired n' ->
      let me = constants.me in
      if n' < n then
	begin
	  log ~me "stable_master: ignoring old lease_expired with n:%s < n:%s" 
	    (Sn.string_of n') (Sn.string_of n) >>= fun () ->
	  Lwt.return (Stable_master (v',n,new_i))
	end
      else
	begin
	  log ~me "stable_master: half-lease_expired: update lease." >>= fun () ->
	  let v = Update.make_update_value (Update.make_master_set me None) in
	  (* TODO: we need election timeout as well here *)
	  Lwt.return (Master_dictate (None,v,n,new_i))
	end
    | FromClient (vo, finished) ->
      begin
	match vo with
	  | None ->
	    begin
	      finished Store.Stop >>= fun () ->
	      Lwt.fail (Failure "forced_stop")
	    end
	  | Some value ->
	    begin
	      Lwt.return (Master_dictate (Some finished,value,n,new_i))
	    end
      end
    | FromNode (msg,source) ->
      begin
	let me = constants.me in
	match msg with
	  | Prepare (n',i') ->
	    begin
              if am_forced_master constants me
	      then
		if n' > 0L 
		then
		  let new_n = update_n constants n' in
		  Lwt.return (Forced_master_suggest (new_n,new_i))
		else
		  Lwt.return (Stable_master (v',n,new_i) )
	      else
		begin
		  handle_prepare constants source n n' i' >>= function
		    | Nak_sent 
		    | Prepare_dropped ->
                      Lwt.return  (Stable_master (v',n,new_i) )
		    | Promise_sent_up2date ->
		      begin
			let tlog_coll = constants.tlog_coll in
			tlog_coll # get_last_i () >>= fun tlc_i ->
			tlog_coll # get_last_update tlc_i >>= fun l_update ->
			let l_uval = 
			  begin
			    match l_update with 
			      | Some u -> Some( ( Update.make_update_value u ), tlc_i ) 
			      | None -> None
			  end in
			Lwt.return (Slave_wait_for_accept (n', new_i, None, l_uval))
		      end
		    | Promise_sent_needs_catchup ->
                      Store.get_catchup_start_i constants.store >>= fun i ->
                      Lwt.return (Slave_discovered_other_master (source, i, n', i'))
		end
	    end
	  | _ ->
	    begin
	      log ~me "stable_master received %S: dropping" (string_of msg)
	      >>= fun () ->
	      Lwt.return (Stable_master (v',n,new_i))
		
	    end
      end
    | ElectionTimeout n' -> 
      let me = constants.me in
      log ~me "ignoring election timeout (%s)" (Sn.string_of n') >>= fun () ->
      Lwt.return (Stable_master (v',n,new_i))

(* a master informes the others of a new value by means of Accept
   messages and then waits for Accepted responses *)

let master_dictate constants (mo,v,n,i) () =
  constants.on_accept (v,n,i) >>= fun v ->
  begin
  let u = Update.update_from_value v in
  match u with 
    | Update.MasterSet(m,l) ->
      start_lease_expiration_thread constants n (constants.lease_expiration / 2) 
    | _ -> Lwt.return ()
  end >>= fun () ->
  mcast constants (Accept(n,i,v)) >>= fun () ->
  let me = constants.me in
  let others = constants.others in
  let needed = constants.quorum_function (List.length others + 1) in
  let needed' = needed - 1 in
  let ballot = (needed' , [me] ) in
  log ~me "master_dictate n:%s i:%s needed:%d" 
    (Sn.string_of n) (Sn.string_of i) needed' >>= fun () ->
  Lwt.return (Accepteds_check_done (mo, n, i, ballot, v))
