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

open Backend
open Statistics
open Node_cfg
open Lwt
open Log_extra
open Hotc
open Otc
open Update
open Mp_msg
open Common
open Store


let _s_ = function
  | Some x -> "Some " ^ x
  | None -> "None"

exception Forced_stop
let make_went_well stats_cb awake sleeper =
  fun b ->
    begin
		Lwt_log.debug "went_well" >>= fun () ->
		Lwt.catch ( fun () ->
			Lwt.return (Lwt.wakeup awake b)
		) ( fun e ->
		match e with 
			| Invalid_argument s ->		
				let t = state sleeper in
				begin
				match t with
					| Fail ex ->
						Lwt_log.error "Lwt.wakeup error: Sleeper already failed before. Re-raising" >>= fun () ->
						Lwt.fail ex
					| Return v ->
						Lwt_log.error "Lwt.wakeup error: Sleeper already returned"
					| Sleep ->
						Lwt.fail (Failure "Lwt.wakeup error: Sleeper is still sleeping however")
				end
			| _ -> Lwt.fail e
		) >>= fun () ->
		stats_cb ();
		Lwt_log.debug "went_well finished" >>= fun () ->
		Lwt.return ()
		end





class sync_backend cfg push_update 
  (store:Store.store) 
  (tlog_collection:Tlogcollection.tlog_collection) 
  (lease_expiration:int)
  ~quorum_function n_nodes
  ~expect_reachable
  ~test 
  =
  let my_name =  Node_cfg.node_name cfg in
  let locked_tlogs = Hashtbl.create 8 in
  let blockers_cond = Lwt_condition.create() in
  
object(self: #backend)
  val instantiation_time = Int64.of_float (Unix.time())
  val witnessed = Hashtbl.create 10 
  val _stats = Statistics.create ()

  method exists key =
    log_o self "exists %s" key >>= fun () ->
    self # _only_if_master () >>= fun () ->
    store # exists key

  method get key = log_o self "get %s" key >>= fun () ->
    self # _only_if_master () >>= fun () ->
    Lwt.catch
      (fun () -> 
	store # get key >>= fun v -> 
	Statistics.new_get _stats key v; 
	Lwt.return v)
      (fun exc ->
	match exc with
	  | Not_found ->
	    Lwt.fail (Common.XException (Arakoon_exc.E_NOT_FOUND, key))
	  | Store.CorruptStore ->
	    begin
	      Lwt_log.fatal "CORRUPT_STORE" >>= fun () ->
	      Lwt.fail Server.FOOBAR
	    end
	  | ext -> Lwt.fail ext)


  method private _update_rendezvous update update_stats = 
    self # _only_if_master () >>= fun () ->
    let p_value = Update.make_update_value update in
    let sleep, awake = Lwt.wait () in
    let went_well = make_went_well update_stats awake sleep in
    push_update (Some p_value, went_well) >>= fun () ->
    sleep >>= function
      | Store.Stop -> Lwt.fail Forced_stop
      | Store.Update_fail (rc,str) -> Lwt.fail (XException(rc,str))
      | Store.Ok _ -> Lwt.return ()

  method private block_collapser (i: Sn.t) =
    let tlog_file_n = Tlc2.get_file_number i  in
    Hashtbl.add locked_tlogs tlog_file_n "locked"
    
  method private unblock_collapser i =
    let tlog_file_n = Tlc2.get_file_number i in
    Hashtbl.remove locked_tlogs tlog_file_n;
    Lwt_condition.signal blockers_cond () 
    
  method private wait_for_tlog_release tlog_file_n =
    let blocking_requests = [] in
    let maybe_add_blocker tlog_num s blockers =
      if tlog_file_n >= tlog_num 
      then
        tlog_num :: blockers 
      else 
        blockers
    in
    let blockers = Hashtbl.fold  maybe_add_blocker locked_tlogs blocking_requests in
    if List.length blockers > 0 then
      Lwt_condition.wait blockers_cond >>= fun () ->
      self # wait_for_tlog_release tlog_file_n
    else 
      Lwt.return ()
  
  method range (first:string option) finc (last:string option) linc max =
    log_o self "%s %b %s %b %i" (_s_ first) finc (_s_ last) linc max >>= fun () ->
    self # _only_if_master () >>= fun () ->
    store # range first finc last linc max

  method last_entries (start_i:Sn.t) (oc:Lwt_io.output_channel) =
  
    Lwt.finalize(
      fun () ->
        begin
          self # block_collapser start_i ;
          self # _last_entries start_i oc
        end
    ) (
      fun () ->
        Lwt.return ( self # unblock_collapser start_i )  
    )
  
  method range_entries (first:string option) finc (last:string option) linc max =
    log_o self "%s %b %s %b %i" (_s_ first) finc (_s_ last) linc max >>= fun () ->
    self # _only_if_master () >>= fun () ->
    store # range_entries first finc last linc max

  method prefix_keys (prefix:string) (max:int) =
    log_o self "prefix_keys %s %d" prefix max >>= fun () ->
    self # _only_if_master () >>= fun () ->
    store # prefix_keys prefix max >>= fun key_list ->
    Lwt_log.debug_f "prefix_keys found %d matching keys" (List.length key_list) >>= fun () ->
    Lwt.return key_list

  method set key value =
    log_o self "set %S %S" key value >>= fun () ->
    let update = Update.Set(key,value) in    
    let update_sets () = Statistics.new_set _stats key value in
    self # _update_rendezvous update update_sets

  method test_and_set key expected (wanted:string option) =
    log_o self "test_and_set %s %s %s" key 
      (string_option_to_string expected) 
      (string_option_to_string wanted)
    >>= fun () ->
    self # _only_if_master () >>= fun () ->
    let update = Update.TestAndSet(key, expected, wanted) in
    let p_value = Update.make_update_value update in
    let sleep, awake = Lwt.wait () in
    let went_well = make_went_well (fun () -> ()) awake sleep in
    push_update (Some p_value, went_well) >>= fun () ->
    sleep >>= function
      | Store.Stop -> Lwt.fail Forced_stop
      | Store.Update_fail (rc,str) -> Lwt.fail (Failure str)
      | Store.Ok x -> Lwt.return x

  method delete key = log_o self "delete %S" key >>= fun () ->
    self # _only_if_master ()>>= fun () ->
    let update = Update.Delete key in
    let update_stats () = Statistics.new_delete _stats in
    self # _update_rendezvous update update_stats 

  method hello (client_id:string) (cluster_id:string) =
    log_o self "hello %S %S" client_id cluster_id >>= fun () -> 
    let msg = Printf.sprintf "Arakoon %S" Version.version in
    Lwt.return (0l, msg)

  method private _last_entries (start_i:Sn.t) (oc:Lwt_io.output_channel) =
    log_o self "last_entries %s" (Sn.string_of start_i) >>= fun () ->
    store # consensus_i () >>= fun consensus_i ->
    begin 
      match consensus_i with
	| None -> Lwt.return () 
	| Some ci ->
	  begin
	    tlog_collection # get_infimum_i () >>= fun inf_i ->
	    let too_far_i = Sn.succ ci in
	    log_o self 
	      "inf_i:%s too_far_i:%s" (Sn.string_of inf_i)
	      (Sn.string_of too_far_i)
	    
	    >>= fun () ->
	    begin
	      if start_i < inf_i 
	      then 
		begin
		  Llio.output_int oc 2 >>= fun () ->
		  tlog_collection # dump_head oc
		end
	      else 
		begin
		  Llio.output_int oc 1 >>= fun () -> Lwt.return start_i
		end
	    end
	    >>= fun start_i'->
	    let f(i,u) = Tlogcommon.write_entry oc i u in
	    tlog_collection # iterate start_i' too_far_i f >>= fun () ->
	    Sn.output_sn oc (-1L) 
	  end
    end
    >>= fun () ->
    log_o self "done with_last_entries"


  method sequence (updates:Update.t list) =
    log_o self "sequence" >>= fun () ->
    let update = Update.Sequence updates in
    let update_stats () = Statistics.new_sequence _stats in
    self # _update_rendezvous update update_stats

  method multi_get (keys:string list) =
    log_o self "multi_get" >>= fun () ->
    self # _only_if_master () >>= fun () ->
    store # multi_get keys >>= fun values ->
    Statistics.new_multiget _stats;
    Lwt.return values

  method to_string () = "sync_backend(" ^ (Node_cfg.node_name cfg) ^")"

  method private _who_master () =
    store # who_master ()

  method who_master () =
    log_o self "who_master" >>= fun () ->
    self # _who_master () >>= fun mo ->
    let result,argumentation = 
      match mo with
	| None -> None,"young cluster"
	| Some (m,ls) ->
	  match Node_cfg.forced_master cfg with
	    | None ->
	      begin
	      if (m = my_name ) && (ls < instantiation_time)
	      then None, (Printf.sprintf "%Li considered invalid lease from previous incarnation" ls)
	      else
		let now = Int64.of_float (Unix.time()) in
		let diff = Int64.sub now ls in
		if diff < Int64.of_int lease_expiration then
		  (Some m,"inside lease")
		else (None,Printf.sprintf "(%Li < (%Li = now) lease expired" ls now)
	      end
	    | x -> x,"forced master"
    in
    log_o self "master:%s (%s)" (string_option_to_string result) argumentation 
    >>= fun () ->
    Lwt.return result

  method private _only_if_master() =
    self # who_master () >>= function
      | None ->
	Lwt.fail (XException(Arakoon_exc.E_NOT_MASTER, "None"))
      | Some m ->
	if m <> my_name
	then
	  Lwt.fail (XException(Arakoon_exc.E_NOT_MASTER, m))
	else
	  Lwt.return ()

  method witness name i = 
    Hashtbl.replace witnessed name i;
    Lwt_log.debug_f "witnessed (%s,%s)" name (Sn.string_of i) >>= fun () ->
    let node_is = Hashtbl.fold (fun k v acc -> (k,v)::acc) witnessed [] in
    Statistics.new_node_is _stats node_is;
    Lwt.return ()

  method expect_progress_possible () = 
    store # consensus_i () >>= function 
      | None -> Lwt.return false
      | Some i ->
	let q = quorum_function n_nodes in
	let count,s = Hashtbl.fold
	  (fun name ci (count,s) -> 
	    let s' = s ^ Printf.sprintf " (%s,%s) " name (Sn.string_of ci) in
	    if (expect_reachable ~target:name) && 
	      (ci = i || (Sn.pred ci) = i )
	    then  count+1,s' 
	    else  count,s') 
	  witnessed (1,"") in
	let v = count >= q in
	Lwt_log.debug_f "count:%i,q=%i i=%s detail:%s" count q (Sn.string_of i) s
	>>= fun () ->
	Lwt.return v

  method get_statistics () = _stats

  method check ~cluster_id = 
    let r = test ~cluster_id in
    Lwt.return r

  method collapse n cb = 
    let tlog_dir = Node_cfg.tlog_dir cfg in
    let new_cb tlog_name =
      let file_num = Tlc2.get_number tlog_name in
      cb() >>= fun () ->
      self # wait_for_tlog_release (Sn.of_int file_num) 
    in
    Collapser_main.collapse_lwt tlog_dir n new_cb >>= fun () ->
    Lwt.return ()
end
