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

open Lwt
open Messaging
open Mp_msg
open MPMessage

open Update
open Lwt_buffer
open Fsm
open Multi_paxos_type
open Multi_paxos
open Master_type


(* a forced master always suggests himself *)
let forced_master_suggest constants (n,i) () =
  let me = constants.me in
  let n' = update_n constants n in
  mcast constants (Prepare (n',i)) >>= fun () ->
  start_election_timeout constants n >>= fun () ->
  Logger.debug_f_ "%s: forced_master_suggest: suggesting n=%s" me (Sn.string_of n') >>= fun () ->
  let tlog_coll = constants.tlog_coll in
  let l_val = tlog_coll # get_last_value i in
  
  let v_lims =
    begin
      match l_val with
        | None -> (1,[]) 
        | Some v ->(0, [v,1])      
    end 
  in
  let who_voted = [me] in
  
  let i_lim = Some (me,i) in
  let state = (n', i, who_voted, v_lims, i_lim, []) in
  Fsm.return (Promises_check_done state)

(* in case of election, everybody suggests himself *)
let election_suggest constants (n,i,vo) () =
  let me = constants.me in
  let v_lims, msg = 
    match vo with 
      | None ->
        (1,[]) , "None"
      | Some x -> (0,[(x,1)]) , "Some _"
  in
  Logger.debug_f_ "%s: election_suggest: n=%s i=%s %s" me  (Sn.string_of n) (Sn.string_of i) msg >>= fun () ->
  start_election_timeout constants n >>= fun () ->
  let delay =
    match constants.master with
      | Preferred ps when not (List.mem me ps) -> 1 + (constants.lease_expiration /2)
      | _ -> 0
  in
  let df = float delay in
  Lwt_extra.ignore_result 
    (Lwt_unix.sleep df >>= fun () -> mcast constants (Prepare (n,i)));
  let who_voted = [me] in
  let i_lim = Some (me,i) in
  let state = (n, i, who_voted, v_lims, i_lim, []) in
  Fsm.return (Promises_check_done state)

let read_only constants state () =
  Lwt_unix.sleep 60.0 >>= fun () ->
  Logger.debug_f_ "%s: read_only ..." constants.me >>= fun () ->
  Fsm.return (Read_only state)

(* a pending slave that is waiting for a prepare or a nak
   in order to discover a master *)
let slave_waiting_for_prepare (type s) constants ( (current_i:Sn.t),(current_n:Sn.t)) event =
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  match event with 
    | FromNode(msg,source) ->
      begin
	Logger.debug_f_ "%s: slave_waiting_for_prepare: %S" constants.me (MPMessage.string_of msg) 
	>>= fun () ->
	match msg with
	  | Prepare(n',i') ->
	    begin
          handle_prepare constants source current_n n' i' >>= function
		    | Nak_sent
		    | Prepare_dropped ->
		        Fsm.return ( Slave_waiting_for_prepare(current_i, current_n ) )
		    | Promise_sent_up2date ->
                begin
		          let last = constants.tlog_coll # get_last () in
		          Fsm.return (Slave_wait_for_accept (n', current_i, None, last))
                end
		    | Promise_sent_needs_catchup ->
		        let i = S.get_catchup_start_i constants.store in
		        let state = (source, i, n',i') in 
		        Fsm.return (Slave_discovered_other_master state)
	    end
	  | Nak(n',(n2, i2)) when n' = -1L ->
	      begin
	        Logger.debug_f_ "%s: fake prepare response: discovered master" constants.me >>= fun () ->
            let cu_pred = S.get_catchup_start_i constants.store in
            Fsm.return (Slave_discovered_other_master (source, cu_pred, n2, i2))
	      end
	  | Nak(n',(n2, i2)) when i2 > current_i ->
	      begin
	        Logger.debug_f_ "%s: got %s => go to catchup" constants.me (string_of msg) >>= fun () ->
            let cu_pred =  S.get_catchup_start_i constants.store in
            Fsm.return (Slave_discovered_other_master (source, cu_pred, n2, i2))
	      end
	  | Nak(n',(n2, i2)) when i2 = current_i ->
	      begin
	        Logger.debug_f_ "%s: got %s => we're in sync" constants.me (string_of msg) >>= fun () ->
          (* pick in @ steady state *)
	        let p = constants.get_value i2 in
	        match p with
              | None ->
                  begin
	                Fsm.return (Slave_waiting_for_prepare (i2,current_n) )
                  end
              | Some v ->
                  begin
                    Logger.debug_f_ "%s: reentering steady state @(%s,%s)" constants.me 
		              (Sn.string_of n2) (Sn.string_of i2) 
                    >>= fun () ->
                    start_lease_expiration_thread constants n2 constants.lease_expiration >>= fun () ->
                    Fsm.return (Slave_steady_state (n2, i2, v))
	              end
	      end
      | Accept(n', i', v) when current_n = n' && i' > current_i ->
          begin
            let cu_pred = S.get_catchup_start_i constants.store in
            Fsm.return (Slave_discovered_other_master (source, cu_pred, n', i'))
          end
	  | _ -> Logger.debug_f_ "%s: dropping unexpected %s" constants.me (string_of msg) >>= fun () ->
	      Fsm.return (Slave_waiting_for_prepare (current_i,current_n))
      end
    | ElectionTimeout n' 
    | LeaseExpired n' ->
        if n' = current_n 
        then Fsm.return (Slave_fake_prepare(current_i, current_n))
        else Fsm.return (Slave_waiting_for_prepare(current_i, current_n))
    | FromClient _ -> paxos_fatal constants.me "Slave_waiting_for_prepare cannot handle client requests"
    
    | Quiesce (sleep,awake) ->
        handle_quiesce_request (module S) constants.store sleep awake >>= fun () ->
        Fsm.return (Slave_waiting_for_prepare (current_i,current_n) )
          
    | Unquiesce ->
        handle_unquiesce_request constants current_n >>= fun (store_i, vo) ->
        Fsm.return (Slave_waiting_for_prepare (current_i,current_n) )
    | DropMaster (sleep, awake) ->
        Multi_paxos.safe_wakeup sleep awake () >>= fun () ->
        Fsm.return (Slave_waiting_for_prepare (current_i, current_n))


(* a potential master is waiting for promises and if enough
   promises are received the value is accepted and Accept is broadcasted *)
let promises_check_done constants state () =
  let n, i, who_voted, (v_lims:v_limits), i_lim, lease_expire_waiters = state in
  (* 
     3 cases:
     -) too early to decide
     -) consensus on some value (either my wanted or another)
     -) no consensus possible anymore. (split vote)
  *)
  let me = constants.me in
  let nnones, v_s = v_lims in 
  let bv,bf,number_of_updates =
  begin 
    match v_s with 
      | [] ->  (Value.create_master_value (me, 0L), 0, 1)
      | hd::tl -> 
        let bv, bf = hd in
        if Value.is_master_set bv 
        then (Value.create_master_value (me, 0L), bf, 1)
        else bv, bf , List.length (Value.updates_from_value bv)
         
  end in 
  let nnodes = List.length constants.others + 1 in
  let needed = constants.quorum_function nnodes in
  let nvoted = List.length who_voted in
  if bf + nnones = needed 
  then
    begin
      Logger.debug_f_ "%s: promises_check_done: consensus on %s" me (Sn.string_of i)
      >>= fun () ->
      constants.on_accept (bv,n,i) >>= fun () ->
      start_lease_expiration_thread constants n (constants.lease_expiration / 2)  >>= fun () ->
      let msg = Accept(n,i,bv) in
      mcast constants msg >>= fun () ->
      let new_ballot = (needed-1 , [me] ) in
      let ff = fun _ -> Lwt.return () in
      let ffs =
        let rec repeat = function
          | 0 -> []
          | n -> ff :: repeat (n - 1) in
        repeat number_of_updates in
      Fsm.return (Accepteds_check_done (ffs, n, i, new_ballot, bv, lease_expire_waiters))
    end
  else (* bf < needed *)
    if nvoted < nnodes 
    then Fsm.return (Wait_for_promises state)
    else (* split vote *)
      begin
    	if am_forced_master constants me
	    then
	      Fsm.return (Forced_master_suggest (n,i))
	    else 
	      if is_election constants 
	      then
	        let n' = update_n constants n in
	        Fsm.return (Election_suggest (n',i, Some bv))
	      else
	        paxos_fatal me "slave checking for promises"
      end
	

(* a potential master is waiting for promises and receives a msg *)
let wait_for_promises (type s) constants state event =
  let me = constants.me in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  let (n, i, who_voted, v_lims, i_lim, lease_expire_waiters) = state in
  match event with
    | FromNode (msg,source) ->
      begin
        let wanted =
        begin
          let nnones, nsomes = v_lims in
          match nsomes with
            | [] -> None
            | hd::tl -> 
              let bv, bf = hd in
              Some bv
        end
        in
	    let who_voted_s = Log_extra.list2s (fun s -> s) who_voted in

        let log_e0 = 
          ELog(fun () ->
            Printf.sprintf "wait_for_promises:n=%s i=%s who_voted = %s received %s from %s" 
              (Sn.string_of n) (Sn.string_of i) who_voted_s 
              (string_of msg) source
          )
        in
        let drop msg reason = 
          let log_e = ELog(fun () ->
            Printf.sprintf "dropping %s because: %s" (string_of msg) reason) 
          in
          Fsm.return ~sides:[log_e0;log_e] (Wait_for_promises state) 
        in
        begin
          match msg with
            | Promise (n' ,i', limit) when n' < n ->
                let reason = Printf.sprintf "old promise (%s < %s)" (Sn.string_of n') (Sn.string_of n) in
                drop msg reason
            | Promise (n',i', limit) when n' = n && i' < i ->
                let reason = Printf.sprintf "old promise with lower i: %s < %s"
                  (Sn.string_of i') (Sn.string_of i)
                in
                drop msg reason
            | Promise (n' ,new_i, limit) when n' = n ->
                if List.mem source who_voted then
                  drop msg "duplicate promise"
                else
                  let v_lims' = 
                    begin
                      if new_i < i then
                        let (nnones, nsomes) = v_lims in
                        (nnones+1, nsomes)
                      else
                        update_votes v_lims limit 
                    end in
                  let who_voted' = source :: who_voted in
                  let new_ilim = match i_lim with
                    | Some (source',i') -> if i' < new_i then Some (source,new_i) else i_lim
                    | None -> Some (source,new_i)
		          in
                  let state' = (n, i, who_voted', v_lims', new_ilim, lease_expire_waiters) in
                  Fsm.return (Promises_check_done state')
            | Promise (n' ,i', limit) -> (* n ' > n *)
                begin
		          Logger.debug_f_ "%s: Received Promise from previous incarnation. Bumping n from %s over %s." me 
                    (Sn.string_of n) (Sn.string_of n') 
		          >>= fun () ->
		          let new_n = update_n constants n' in
		          Fsm.return (Election_suggest (new_n,i, wanted))
                end
            | Nak (n',(n'',i')) when n' < n ->
                begin
		          Logger.debug_f_ "%s: wait_for_promises:: received old %S (ignoring)" me (string_of msg) 
		          >>= fun () ->
		          Fsm.return (Wait_for_promises state)
                end
            | Nak (n',(n'',i')) when n' > n ->
                begin
		          Logger.debug_f_ "%s: Received Nak from previous incarnation. Bumping n from %s over %s." me 
                    (Sn.string_of n) (Sn.string_of n') 
		          >>= fun () ->
		          let new_n = update_n constants n' in
		          Fsm.return (Election_suggest (new_n,i, wanted))
                end
            | Nak (n',(n'',i')) -> (* n' = n *)
                begin
		          Logger.debug_f_ "%s: wait_for_promises:: received %S for my Prep(i=%s, _) " me 
                    (string_of msg) 
                    (Sn.string_of i)
		          >>= fun () ->
		          if am_forced_master constants me
		          then
		            begin
                      Logger.debug_f_ "%s: wait_for_promises; forcing new master suggest" me >>= fun () ->
                      let n3 = update_n constants (max n n'') in
                      Fsm.return (Forced_master_suggest (n3,i))
		            end
		          else 
                    (* if is_election constants 
                       then *)
                    begin
                      Logger.debug_f_ "%s: wait_for_promises; discovered other node" me 
                      >>= fun () ->
                      if i' > i 
                      then
                        let cu_pred = S.get_catchup_start_i constants.store in
                        Fsm.return (Slave_discovered_other_master (source,cu_pred,n'',i'))
                      else
			            let new_n = update_n constants (max n n'') in
			            Fsm.return (Election_suggest (new_n,i, wanted))
                    end
                 (* else (* forced_slave *) (* this state is impossible?! *)
                    begin
                      Logger.debug_f_ "%s: wait_for_promises; forced slave back waiting for prepare" me >>= fun () ->
                      Lwt.return (Slave_waiting_for_prepare (i,n))
                    end *)
              end
            | Prepare (n',i') ->
              begin
                if (am_forced_master constants me) && n' > 0L
	              then
		              begin
		                Logger.debug_f_ "%s: wait_for_promises:dueling; forcing new master suggest" me >>= fun () ->
		                let reply = Nak (n', (n,i))  in
				        constants.send reply me source >>= fun () ->
				        let new_n = update_n constants n' in
				        Fsm.return (Forced_master_suggest (new_n, i))
				      end
		        else
                  
                  handle_prepare constants source n n' i' >>= function
                    | Nak_sent ->
                      Logger.debug_f_ "%s: wait_for_promises: resending prepare" me >>= fun () ->
                      let reply = Prepare(n, i) in
                      constants.send reply me source >>= fun () ->
                      Fsm.return (Wait_for_promises state)
                    | Prepare_dropped -> 
                      Fsm.return (Wait_for_promises state)
                    | Promise_sent_up2date ->
		                begin
                          let last = constants.tlog_coll # get_last () in
			              Fsm.return (Slave_wait_for_accept (n', i, None, last))
		                end
		            | Promise_sent_needs_catchup ->
		              let i = S.get_catchup_start_i constants.store in
		              Fsm.return (Slave_discovered_other_master (source, i, n', i'))
              end
            | Accept (n',_i,_v) when n' < n ->
                begin
                  if i < _i
                  then
                    begin
                      Logger.debug_f_ "%s: wait_for_promises: still have an active master (received %s) -> catching up from master" me  (string_of msg) >>= fun () ->
                      Fsm.return (Slave_discovered_other_master (source, i, n', _i))
		            end
                else
		            Logger.debug_f_ "%s: wait_for_promises: ignoring old Accept %s" me (Sn.string_of n') 
		             >>= fun () ->
		          Fsm.return (Wait_for_promises state)
              end
            | Accept (n',_i,_v) ->
              if n' = n && _i < i 
              then
		        begin
                  Logger.debug_f_ "%s: wait_for_promises: ignoring %s (my i is %s)" me (string_of msg) (Sn.string_of i) >>= fun () ->
                  Fsm.return (Wait_for_promises state)
		        end
              else
		        begin
                  Logger.debug_f_ "%s: wait_for_promises: received %S -> back to fake prepare" me  (string_of msg) >>= fun () ->
                  Fsm.return (Slave_fake_prepare (i,n))
		        end
            | Accepted (n',_i) when n' < n ->
              begin
		        Logger.debug_f_ "%s: wait_for_promises: ignoring old Accepted %s" me (Sn.string_of n') >>= fun () ->
		        Fsm.return (Wait_for_promises state)
              end
            | Accepted (n',_i) -> (* n' >= n *)
              begin 
		        Logger.debug_f_ "%s: Received Nak from previous incarnation. Bumping n from %s over %s." me (Sn.string_of n) (Sn.string_of n') 
		        >>= fun () ->
		        let new_n = update_n constants n' in
		        Fsm.return (Election_suggest (new_n,i, wanted))
              end
        end
      end
    | ElectionTimeout n' ->
      let (n,i,who_voted, v_lims, i_lim, lease_expire_waiters) = state in
      let wanted =
        begin
          let nnones, nsomes = v_lims in
          match nsomes with
            | [] -> None
            | hd::tl -> 
              let bv, bf = hd in
              Some bv
        end
        in
      if n' = n && not ( S.quiesced constants.store )
      then
	    begin
          Logger.debug_f_ "%s: wait_for_promises: election timeout, restart from scratch" me	  
	      >>= fun () ->
	      Fsm.return (Election_suggest (n,i, wanted))
	    end
      else
	    begin
	      Fsm.return (Wait_for_promises state)
	    end
    | LeaseExpired _ ->
        Logger.debug_f_ "%s: Ignoring lease expiration" me >>= fun () ->
        Fsm.return (Wait_for_promises state)
    | FromClient _ -> 
        paxos_fatal me "wait_for_promises: don't want FromClient"
          
    | Quiesce (sleep,awake) ->
        handle_quiesce_request (module S) constants.store sleep awake >>= fun () ->
        Fsm.return (Wait_for_promises state)
      
    | Unquiesce ->
        handle_unquiesce_request constants n >>= fun (store_i, vo) ->
        Fsm.return (Wait_for_promises state)
    | DropMaster (sleep, awake) ->
        Multi_paxos.safe_wakeup sleep awake () >>= fun () ->
        Fsm.return (Wait_for_promises state)
      
  
  
(* a (potential or full) master is waiting for accepteds and if he has received
   enough, consensus is reached and he becomes a full master *)
let lost_master_role finished_funs =
  begin
	let msg = "lost master role during wait_for_accepteds while handling client request" in
	let rc = Arakoon_exc.E_NOT_MASTER in
	let result = Store.Update_fail (rc, msg) in
    let rec loop = function
      | [] -> Lwt.return ()
      | f::ffs -> f result >>= fun () -> loop ffs
    in
	loop finished_funs
  end

let accepteds_check_done constants state () =
  let (mo,n,i,ballot,v, lease_expire_waiters) = state in
  let needed, already_voted = ballot in
  if needed = 0 
  then
    begin
      let log_e = 
        ELog (fun () ->
          Printf.sprintf "accepted_check_done :: we're done! returning %s %s"
	        (Sn.string_of n) ( Sn.string_of i )
        )
      in
      let sides = [log_e] in
      Fsm.return ~sides (Master_consensus (mo,v,n,i, lease_expire_waiters))
    end
  else
    Fsm.return (Wait_for_accepteds state)


(* a (potential or full) master is waiting for accepteds and receives a msg *)
let wait_for_accepteds (type s) constants state (event:paxos_event) =
  let me = constants.me in
  let module S = (val constants.store_module : Store.STORE with type t = s) in
  match event with
    | FromNode(msg,source) ->
      begin
        (* TODO: what happens with the client request
           when I fall back to a state without mo ? *)
	    let (mo,n,i,ballot,v, lease_expire_waiters) = state in
	    let drop msg reason =
          let log_e = ELog 
            (fun () ->
	          Printf.sprintf "dropping %s because : '%s'" (MPMessage.string_of msg) reason
            )
          in	      
	      Fsm.return ~sides:[log_e] (Wait_for_accepteds state)
	    in
	    let needed, already_voted = ballot in
	    Logger.debug_f_ "%s: wait_for_accepteds(%i to go) got %S from %s" me needed 
	      (MPMessage.string_of msg) source >>= fun () ->
        begin
	      match msg with
	        | Accepted (n',i') when (n',i')=(n,i)  ->
	            begin
	              let () = constants.on_witness source i' in 
	              if List.mem source already_voted then
		            let reason = Printf.sprintf "%s already voted" source in
		            drop msg reason
	              else
		            let ballot' = needed -1, source :: already_voted in
		            Fsm.return (Accepteds_check_done (mo,n,i,ballot',v, lease_expire_waiters))
	            end
	        | Accepted (n',i') when n' = n && i' < i ->
	            begin
	              let () = constants.on_witness source i' in
	              Logger.debug_f_ "%s: wait_for_accepteds: received older Accepted for my n: ignoring" me 
	              >>= fun () ->
	              Fsm.return (Wait_for_accepteds state)
	            end
	        | Accepted (n',i') when n' < n ->
	            begin
	              let () = constants.on_witness source i' in
	              let reason = Printf.sprintf "dropping old %S we're @ (%s,%s)" (string_of msg)
		            (Sn.string_of n) (Sn.string_of i) in
	              drop msg reason
	            end
	        | Accepted (n',i') -> (* n' > n *)
	            paxos_fatal me "wait_for_accepteds:: received %S with n'=%s > my n=%s FATAL" (string_of msg) (Sn.string_of n') (Sn.string_of n)
	              
	        | Promise(n',i', limit) ->
	            begin
	              let () = constants.on_witness source i' in
	              if n' <= n then
                    begin
			          let reason = Printf.sprintf
				        "already reached consensus on (%s,%s)" 
				        (Sn.string_of n) (Sn.string_of i) 
			          in
			          drop msg reason
			        end
	              else
		            begin
		              let reason = Printf.sprintf "future Promise(%s,%s), local (%s,%s)"
			            (Sn.string_of n') (Sn.string_of i') (Sn.string_of n) (Sn.string_of i) in
			          drop msg reason
			        end
	            end
	        | Prepare (n',i') -> (* n' > n *)
	            begin
	              if am_forced_master constants me
	              then
                    if n' <= n
                    then
                      let reply = Nak( n', (n,i) ) in
                      constants.send reply me source >>= fun () ->
                      let followup = Accept( n, i, v) in
                      constants.send followup me source >>= fun () ->
                      Fsm.return (Wait_for_accepteds state)
                    else
                      begin
                        lost_master_role mo >>= fun () ->
                        Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
                        Fsm.return (Forced_master_suggest (n',i))
                      end
	              else 
                    begin
                      handle_prepare constants source n n' i' >>= 
                        function
                          | Prepare_dropped
                          | Nak_sent -> 
                              Fsm.return( Wait_for_accepteds state )
                          | Promise_sent_up2date ->
                              begin
                                let last = constants.tlog_coll # get_last () in
                                lost_master_role mo >>= fun () ->
                                Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
				Fsm.return
                                  (Slave_wait_for_accept (n', i, None, last))
                              end
                          | Promise_sent_needs_catchup ->
                              begin
                                let i = S.get_catchup_start_i constants.store in
                                lost_master_role mo >>= fun () ->
                                Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
                                Fsm.return (Slave_discovered_other_master (source, i, n', i'))
                              end
                    end
                end 
	        | Nak (n',i) ->
	            begin
	              Logger.debug_f_ "%s: wait_for_accepted: ignoring %S from %s when collecting accepteds" me 
                    (MPMessage.string_of msg) source >>= fun () ->
	              Fsm.return (Wait_for_accepteds state)
	            end
	        | Accept (n',_i,_v) when n' < n ->
	            begin
	              Logger.debug_f_ "%s: wait_for_accepted: dropping old Accept %S" me (string_of msg) >>= fun () ->
	              Fsm.return (Wait_for_accepteds state)
	            end
	        | Accept (n',i',v') when (n',i',v')=(n,i,v) ->
	            begin
	              Logger.debug_f_ "%s: wait_for_accepted: ignoring extra Accept %S" me (string_of msg) >>= fun () ->
	              Fsm.return (Wait_for_accepteds state)
	            end
	        | Accept (n',i',v') when i' <= i -> (* n' = n *)
                begin
                  Logger.debug_f_ "%s: wait_for_accepteds: dropping accept with n = %s and i = %s" me 
                    (Sn.string_of n) (Sn.string_of i') >>= fun () ->
                  Fsm.return (Wait_for_accepteds state)
                end
	        | Accept (n',i',v') (* n' >= n && i' > i *)->
              (* check lease, if we're inside, drop (how could this have happened?) 
                 otherwise, we've lost master role
              *)
              let is_still_master () =
                match S.who_master constants.store with
                  | None -> false (* ???? *)
                  | Some (_,al) -> let now = Unix.gettimeofday () in
                                   let alf = Int64.to_float al in
                                   let diff = now -. alf in
                                   diff < (float constants.lease_expiration)
              in
              if is_still_master () 
              then
                begin
                  Logger.debug_f_ "%s: wait_for_accepteds: drop %S (it's still me)" me (string_of msg) >>= fun () ->
                  Fsm.return (Wait_for_accepteds state)
                end
              else
                begin
                  lost_master_role mo >>= fun () ->
                  Multi_paxos.safe_wakeup_all () lease_expire_waiters >>= fun () ->
                  begin 
                  (* Become slave, goto catchup *)
                    Logger.debug_f_ "%s: wait_for_accepteds: received Accept from new master %S" me (string_of msg) >>= fun () ->
                    let cu_pred = S.get_catchup_start_i constants.store in
                    let new_state = (source,cu_pred,n,i') in 
                    Logger.debug_f_ "%s: wait_for_accepteds: drop %S (it's still me)" me (string_of msg) >>= fun () ->
                    Fsm.return (Slave_discovered_other_master new_state)
                  end
                end
        end
      end
    | FromClient _       -> paxos_fatal me "no FromClient should get here"
    | LeaseExpired n'    -> paxos_fatal me "no LeaseExpired should get here"
    | ElectionTimeout n' -> 
      begin
	    let (_,n,i,ballot,v, lease_expire_waiters) = state in
        let here = "wait_for_accepteds : election timeout " in
	    if n' < n then
	      begin
            let log_e = 
              ELog (fun () ->
                Printf.sprintf
	              "%s ignoring old timeout %s<%s" 
                  here
                  (Sn.string_of n') (Sn.string_of n) 
              )
            in
	        Fsm.return ~sides:[log_e] (Wait_for_accepteds state)
	      end
	    else if n' = n then
	      begin
		        Logger.debug_f_ "%s: going to RESEND Accept messages" me >>= fun () ->
		        let needed, already_voted = ballot in
		        let msg = Accept(n,i,v) in
		        let silent_others = List.filter (fun o -> not (List.mem o already_voted)) 
		          constants.others in
		        Lwt_list.iter_s (fun o -> constants.send msg me o) silent_others >>= fun () ->
		        mcast constants msg >>= fun () ->
                start_election_timeout constants n >>= fun () ->
                Fsm.return (Wait_for_accepteds state)
	          end
	        else
	          begin
	        Fsm.return (Wait_for_accepteds state)
	      end
      end
	    
    | Quiesce (sleep,awake) ->
      fail_quiesce_request constants.store sleep awake Quiesced_fail_master >>= fun () ->
      Fsm.return (Wait_for_accepteds state)
      
    | Unquiesce ->
      Lwt.fail (Failure "Unexpected unquiesce request while running as master")
      
    | DropMaster (sleep, awake) ->
      let (mo, n, i, ballot, v, lease_expire_waiters) = state in
      let state' = (mo, n, i, ballot, v, (sleep, awake) :: lease_expire_waiters) in
      Fsm.return (Wait_for_accepteds state')


(* the state machine translator *)

type product_wanted =
  | Nop
  | Node_only
  | Full
  | Node_and_inject
  | Node_and_timeout
  | Node_and_inject_and_timeout

let machine constants = 
  let nop = Nop in
  let full = Full in
  let node_and_timeout = Node_and_timeout in
  let node_and_inject_and_timeout = Node_and_inject_and_timeout in
  function
  | Forced_master_suggest state ->
    (Unit_arg (forced_master_suggest constants state), nop)

  | Slave_fake_prepare i ->
    (Unit_arg (Slave.slave_fake_prepare constants i), nop)
  | Slave_waiting_for_prepare state ->
    (Msg_arg (slave_waiting_for_prepare constants state), node_and_inject_and_timeout)
  | Slave_wait_for_accept state ->
    (Msg_arg (Slave.slave_wait_for_accept constants state), node_and_inject_and_timeout)
  | Slave_steady_state state ->
    (Msg_arg (Slave.slave_steady_state constants state), full)
  | Slave_discovered_other_master state ->
    (Unit_arg (Slave.slave_discovered_other_master constants state), nop)

  | Wait_for_promises state ->
    (Msg_arg (wait_for_promises constants state), node_and_inject_and_timeout)
  | Promises_check_done state ->
    (Unit_arg (promises_check_done constants state), nop)
  | Wait_for_accepteds state ->
    (Msg_arg (wait_for_accepteds constants state), node_and_timeout)
  | Accepteds_check_done state ->
    (Unit_arg (accepteds_check_done constants state), nop)

  | Master_consensus state ->
    (Unit_arg (Master.master_consensus constants state), nop)
  | Stable_master state ->
    (Msg_arg (Master.stable_master constants state), full)
  | Master_dictate state ->
    (Unit_arg (Master.master_dictate constants state), nop)

  | Election_suggest state ->
    (Unit_arg (election_suggest constants state), nop)
  | Read_only state -> 
    (Unit_arg (read_only constants state), nop)
  | Start_transition -> failwith "Start_transition?"

let __tracing = ref false

let trace_transition me key =
  if !__tracing 
  then Logger.debug_f_ "%s: new transition: %s" me (show_transition key)
  else Lwt.return ()

type ready_result =
  | Inject_ready
  | Client_ready
  | Node_ready
  | Election_timeout_ready

let prio = function
  | Inject_ready -> 0
  | Election_timeout_ready -> 1
  | Node_ready   -> 2
  | Client_ready -> 3


type ('a,'b,'c) buffers = 
    {client_buffer : 'a Lwt_buffer.t; 
     node_buffer   : 'b Lwt_buffer.t;
     inject_buffer : 'c Lwt_buffer.t;
     election_timeout_buffer: 'c Lwt_buffer.t;
    } 
let make_buffers (a,b,c,d) = {
  client_buffer = a;
  node_buffer = b;
  inject_buffer = c;
  election_timeout_buffer = d;
}

let rec paxos_produce buffers
    constants product_wanted =
  let me = constants.me in
  let ready_from_inject () =
    Lwt_buffer.wait_for_item buffers.inject_buffer >>= fun () ->
    Lwt.return Inject_ready
  in
  let ready_from_client () =
    Lwt_buffer.wait_for_item buffers.client_buffer >>= fun () ->
    Lwt.return Client_ready
  in
  let ready_from_node () =
    Lwt_buffer.wait_for_item buffers.node_buffer >>= fun () ->
    Lwt.return Node_ready
  in
  let ready_from_election_timeout () =
    Lwt_buffer.wait_for_item buffers.election_timeout_buffer >>= fun () ->
    Lwt.return Election_timeout_ready
  in
  let wmsg, waiters =
    match product_wanted with
      | Node_only -> "Node_only",[ready_from_node ();]
      | Full -> "Full", [ready_from_inject();ready_from_node ();ready_from_client ();]
      | Node_and_inject -> "Node_and_inject", [ready_from_inject();ready_from_node ();]
      | Node_and_timeout -> "Node_and_timeout", [ready_from_election_timeout (); ready_from_node();]
      | Node_and_inject_and_timeout ->
	      "Node_and_inject_and_timeout", [ready_from_inject();ready_from_election_timeout () ;ready_from_node()]
      | Nop -> "Nop", failwith "Nop should not happen here"
  in
  Lwt.catch 
    (fun () ->
      Logger.debug_f_ "%s: T:waiting for event (%s)" me wmsg >>= fun () ->
      let t0 = Unix.gettimeofday () in

      Lwt.pick waiters >>= fun ready_list ->

      let t1 = Unix.gettimeofday () in
      let d = t1 -. t0 in 
      Logger.debug_f_ "%s: T:waiting for event took:%f" me d >>= fun () ->

      let get_highest_prio_evt r_list =
        let f acc b = match acc with 
          | None -> Some b
          | Some pb ->
              if prio(b) > prio (pb)
              then acc
              else Some b
        in
        let best = List.fold_left f None r_list in
        match best with
          | Some Inject_ready ->
	          begin
	            Logger.debug_f_ "%s: taking from inject" me >>= fun () ->
	            Lwt_buffer.take buffers.inject_buffer
	          end
	      | Some Client_ready ->
	          begin
	            Lwt_buffer.harvest buffers.client_buffer >>= fun reqs ->
                let event = FromClient reqs in
	            Lwt.return event
	          end
	      | Some Node_ready ->
	          begin
	            Lwt_buffer.take buffers.node_buffer >>= fun (msg,source) ->
	            let msg2 = MPMessage.of_generic msg in
                let t = Unix.gettimeofday () in
                Logger.debug_f_ "%s: %f SEQ: %s => %s : %s" me t source me (Mp_msg.MPMessage.string_of msg2)
                >>= fun () ->
	            Lwt.return (FromNode (msg2,source))
	          end
	      | Some Election_timeout_ready ->
	          begin
	            Logger.debug_f_ "%s: taking from timeout" me >>= fun () ->
	            Lwt_buffer.take buffers.election_timeout_buffer 
	          end
          | None -> 
	          Lwt.fail ( Failure "FSM BAILED: No events ready while there should be" )
      in get_highest_prio_evt ( [ ready_list ] ) 
    ) 
    (fun e -> Logger.debug_f_ "%s: ZYX %s" me (Printexc.to_string e) >>= fun () -> Lwt.fail e)




let _execute_effects constants e =
  match e with
    | ELog build          -> Logger.debug_f_ "%s: %s" constants.me ("PURE: " ^ build ())
    | EMCast msg          -> mcast constants msg
    | EAccept (v,n,i)     -> constants.on_accept (v,n,i) 
    | ESend (msg, target) -> constants.send msg constants.me target
    | EStartLeaseExpiration (v,n, slave) ->
        begin
          if Value.is_master_set v
          then 
            let period = 
              if slave 
              then constants.lease_expiration 
              else constants.lease_expiration / 2 
            in
            start_lease_expiration_thread constants n period
          else Lwt.return ()
        end 
    | EStartElectionTimeout n -> start_election_timeout constants n

    | EConsensus (finished_funs, v,n,i) ->
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
          loop finished_funs urs 
        end 
    | EGen f -> f ()


(* the entry methods *)

let enter_forced_slave constants buffers new_i vo=
  let me = constants.me in
  Logger.debug_f_ "%s: +starting FSM for forced_slave." me >>= fun () ->
  let trace = trace_transition me in
  let produce = paxos_produce buffers constants in
  let new_n = Sn.start in
  
  Lwt.catch 
    (fun () ->
      Fsm.loop ~trace 
        (_execute_effects constants)
        produce 
	(machine constants) (Slave.slave_fake_prepare constants (new_i,new_n))
    ) 
    (fun exn ->
      Logger.warning_ ~exn "FSM BAILED due to uncaught exception" 
      >>= fun () -> Lwt.fail exn
    )

let enter_forced_master constants buffers current_i vo =
  let me = constants.me in
  Logger.debug_f_ "%s: +starting FSM for forced_master." me >>= fun () ->
  let current_n = 0L in
  let trace = trace_transition me in
  let produce = paxos_produce buffers constants in
  Lwt.catch 
    (fun () ->
      Fsm.loop ~trace 
        (_execute_effects constants)
        produce 
	    (machine constants) 
	    (forced_master_suggest constants (current_n,current_i))
    ) 
    (fun e ->
      Logger.debug_f_ "%s: FSM BAILED due to uncaught exception %s" me (Printexc.to_string e)
      >>= fun () -> Lwt.fail e
    )

let enter_simple_paxos constants buffers current_i vo =
  let me = constants.me in
  Logger.debug_f_ "%s: +starting FSM election." me >>= fun () ->
  let current_n = Sn.start in
  let trace = trace_transition me in
  let produce = paxos_produce buffers constants in
  Lwt.catch 
    (fun () ->
      Fsm.loop ~trace 
        (_execute_effects constants)
        produce 
	(machine constants) 
	(election_suggest constants (current_n, current_i, vo))
    ) 
    (fun e ->
      Logger.debug_f_ "%s: FSM BAILED (run_election) due to uncaught exception %s" me 
	(Printexc.to_string e) 
      >>= fun () -> Lwt.fail e
    )

let enter_read_only constants buffers current_i vo =
  let me = constants.me in
  Logger.debug_f_ "%s: +starting FSM for read_only." me >>= fun () ->
  let current_n = 0L in
  let trace = trace_transition me in
  let produce = paxos_produce buffers constants in
  Lwt.catch
    (fun () ->
      Fsm.loop ~trace 
        (_execute_effects constants)
        produce
	    (machine constants)
	    (read_only constants (current_n, current_i, vo))
    )
    (fun exn ->
      Logger.warning_ ~exn "READ ONLY BAILS OUT" >>= fun () ->
      Lwt.fail exn
    )

let expect_run_forced_slave constants buffers expected step_count new_i =
  Logger.debug_f_ "%s: +starting forced_slave FSM with expect" constants.me >>= fun () ->
  let produce = paxos_produce buffers constants in
  Lwt.catch 
    (fun () ->
      Fsm.expect_loop 
        (_execute_effects constants)
        expected step_count Start_transition produce 
	(machine constants) (Slave.slave_fake_prepare constants new_i)
    ) 
    (fun e ->
      Logger.debug_f_ "%s: FSM BAILED due to uncaught exception %s" constants.me (Printexc.to_string e) 
      >>= fun () -> Lwt.fail e
    )

let expect_run_forced_master constants buffers expected step_count current_n current_i =
  let produce = paxos_produce buffers constants in
  Logger.debug_f_ "%s: +starting forced_master FSM with expect" constants.me >>= fun () ->
  Lwt.catch 
    (fun () ->
        Fsm.expect_loop 
          (_execute_effects constants)
          expected step_count Start_transition produce 
	    (machine constants) 
	    (forced_master_suggest constants (current_n,current_i))
    ) 
    (fun e ->
      Logger.debug_f_ "%s: FSM BAILED due to uncaught exception %s" constants.me (Printexc.to_string e)
      >>= fun () -> Lwt.fail e
    )
