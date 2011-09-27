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
open Client_cfg
open Node_cfg
open Lwt
open Log_extra
open Hotc
open Otc
open Update
open Interval
open Mp_msg
open Common
open Store
open Master_type

let _s_ = function
  | Some x -> "Some " ^ x
  | None -> "None"

let ncfg_prefix_b4 = "nursery.cfg."
let ncfg_prefix_2far = "nursery.cfg/"
let ncfg_prefix_b4_o = Some ncfg_prefix_b4
let ncfg_prefix_2far_o = Some ncfg_prefix_2far

exception Forced_stop
let make_went_well stats_cb awake sleeper =
  fun b ->
    begin
      Lwt.catch 
	( fun () ->Lwt.return (Lwt.wakeup awake b)) 
	( fun e ->
	  match e with 
	    | Invalid_argument s ->		
	      let t = state sleeper in
	      begin
		match t with
		  | Fail ex ->
		    begin
		      Lwt_log.error 
			"Lwt.wakeup error: Sleeper already failed before. Re-raising" 
		      >>= fun () ->
		      Lwt.fail ex
		    end
		  | Return v ->
		    Lwt_log.error "Lwt.wakeup error: Sleeper already returned"
		  | Sleep ->
		    Lwt.fail (Failure "Lwt.wakeup error: Sleeper is still sleeping however")
	      end
	    | _ -> Lwt.fail e
	) >>= fun () ->
      stats_cb ();
      Lwt.return ()
    end





class sync_backend cfg push_update push_node_msg
  (store:Store.store) 
  (store_methods: 
     (?read_only:bool -> string -> Store.store Lwt.t) * 
     (string -> string -> bool -> unit Lwt.t) * string )
  (tlog_collection:Tlogcollection.tlog_collection) 
  (lease_expiration:int)
  ~quorum_function n_nodes
  ~expect_reachable
  ~test 
  ~(read_only:bool)
  =
  let my_name =  Node_cfg.node_name cfg in
  let locked_tlogs = Hashtbl.create 8 in
  let blockers_cond = Lwt_condition.create() in
  let collapsing_lock = Lwt_mutex.create() in
  let assert_value_size value = 
    let length = String.length value in
    if length >= (8 * 1024 * 1024) then
      raise (Arakoon_exc.Exception (Arakoon_exc.E_UNKNOWN_FAILURE, 
				    "value too large"))
  in
object(self: #backend)
  val instantiation_time = Int64.of_float (Unix.time())
  val witnessed = Hashtbl.create 10 
  val _stats = Statistics.create ()
  val mutable client_cfgs = None
  
  method exists ~allow_dirty key =
    log_o self "exists %s" key >>= fun () ->
    self # _read_allowed allow_dirty >>= fun () ->
    store # exists key

  method get ~allow_dirty key = 
    let start = Unix.gettimeofday () in
    log_o self "get ~allow_dirty:%b %s" allow_dirty key >>= fun () ->
    self # _read_allowed allow_dirty >>= fun () ->
    self # _check_interval [key] >>= fun () ->
    Lwt.catch
      (fun () -> 
	store # get key >>= fun v -> 
	Statistics.new_get _stats key v start; 
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

  method get_interval () =
    log_o self "get_interval" >>= fun () ->
    self # _read_allowed false >>= fun () ->
    store # get_interval ()

  method private _update_rendezvous update update_stats push = 
    self # _write_allowed () >>= fun () ->
    let p_value = Update.make_update_value update in
    let sleep, awake = Lwt.wait () in
    let went_well = make_went_well update_stats awake sleep in
    push (Some p_value, went_well) >>= fun () ->
    sleep >>= function
      | Store.Stop -> Lwt.fail Forced_stop
      | Store.Update_fail (rc,str) -> Lwt.fail (XException(rc,str))
      | Store.Ok _ -> Lwt.return ()
      
  method private block_collapser (i: Sn.t) =
    let tlog_file_n = tlog_collection # get_tlog_from_i i  in
    Hashtbl.add locked_tlogs tlog_file_n "locked"
    
  method private unblock_collapser i =
    let tlog_file_n = tlog_collection # get_tlog_from_i i in
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
  
  method range ~allow_dirty (first:string option) finc (last:string option) linc max =
    log_o self "%s %b %s %b %i" (_s_ first) finc (_s_ last) linc max >>= fun () ->
    self # _read_allowed allow_dirty >>= fun () ->
    self # _check_interval_range first last >>= fun () ->
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
  
  method range_entries ~allow_dirty 
    (first:string option) finc (last:string option) linc max =
    log_o self "%s %b %s %b %i" (_s_ first) finc (_s_ last) linc max >>= fun () ->
    self # _read_allowed allow_dirty >>= fun () ->
    self # _check_interval_range first last >>= fun () ->
    store # range_entries first finc last linc max

  method prefix_keys ~allow_dirty (prefix:string) (max:int) =
    log_o self "prefix_keys %s %d" prefix max >>= fun () ->
    self # _read_allowed allow_dirty >>= fun () ->
    self # _check_interval [prefix]  >>= fun () ->
    store # prefix_keys prefix max   >>= fun key_list ->
    Lwt_log.debug_f "prefix_keys found %d matching keys" (List.length key_list) >>= fun () ->
    Lwt.return key_list

  method set key value =
    let start = Unix.gettimeofday () in
    log_o self "set %S" key >>= fun () ->
    self # _check_interval [key] >>= fun () ->
    let () = assert_value_size value in
    let update = Update.Set(key,value) in    
    let update_sets () = Statistics.new_set _stats key value start in
    self # _update_rendezvous update update_sets push_update


  method confirm key value =
    log_o self "confirm %S" key >>= fun () ->
    let () = assert_value_size value in
    self # exists ~allow_dirty:false key >>= function
      | true -> 
	begin
	  store # get key >>= fun old_value ->
	  if old_value = value 
	  then Lwt.return ()
	  else self # set key value
	end
      | false -> self # set key value

  method set_routing r = 
    log_o self "set_routing" >>= fun () ->
    let update = Update.SetRouting r in
    let cb () = () in
    self # _update_rendezvous update cb push_update

  method set_routing_delta left sep right =
    log_o self "set_routing_delta" >>= fun () ->
    let update = Update.SetRoutingDelta (left, sep, right) in
    let cb () = () in
    self # _update_rendezvous update cb push_update
    
  method set_interval iv =
    log_o self "set_interval %s" (Interval.to_string iv)>>= fun () ->
    let update = Update.SetInterval iv in
    self # _update_rendezvous update (fun () -> ()) push_update

  method user_function name po = 
    log_o self "user_function %s" name >>= fun () ->
    let update = Update.UserFunction(name,po) in
    let p_value = Update.make_update_value update in
    let sleep, awake = Lwt.wait() in
    let went_well = make_went_well (fun () -> ()) awake sleep in
    push_update (Some p_value, went_well) >>= fun () ->
    sleep >>= function
      | Store.Stop -> Lwt.fail Forced_stop
      | Store.Update_fail(rc,str) -> Lwt.fail(Common.XException (rc,str))
      | Store.Ok x -> Lwt.return x
  
  method aSSert ~allow_dirty (key:string) (vo:string option) = 
    log_o self "aSSert %S ..." key >>= fun () ->
    begin
      let update = Update.Assert(key,vo) in
      let p_value = Update.make_update_value update in
      let sleep,awake = Lwt.wait() in
      let went_well = make_went_well (fun () -> ()) awake sleep in
      push_update (Some p_value, went_well) >>= fun () ->
      sleep >>= fun sr ->
      log_o self "after sleep" >>= fun () ->
      match sr with
	| Store.Stop -> Lwt.fail Forced_stop
	| Store.Update_fail(rc,str) -> 
	  log_o self "Update Fail case (%li)" 
	    (Arakoon_exc.int32_of_rc rc)>>= fun () ->
	  Lwt.fail (Common.XException(rc,str))
	| Store.Ok _ -> 
	  log_o self "Update Ok case" >>= fun () ->
	  Lwt.return ()
    end
      

  method test_and_set key expected (wanted:string option) =
    let start = Unix.gettimeofday() in
    log_o self "test_and_set %s %s %s" key 
      (string_option_to_string expected) 
      (string_option_to_string wanted)
    >>= fun () ->
    let () = match wanted with
      | None -> ()
      | Some w -> assert_value_size w
    in
    self # _write_allowed () >>= fun () ->
    let update = Update.TestAndSet(key, expected, wanted) in
    let p_value = Update.make_update_value update in
    let sleep, awake = Lwt.wait () in
    let update_stats () = Statistics.new_testandset _stats start in 
    let went_well = make_went_well update_stats awake sleep in
    push_update (Some p_value, went_well) >>= fun () ->
    sleep >>= function
      | Store.Stop -> Lwt.fail Forced_stop
      | Store.Update_fail (rc,str) -> Lwt.fail (Failure str)
      | Store.Ok x -> Lwt.return x

  method delete key = log_o self "delete %S" key >>= fun () ->
    let start = Unix.gettimeofday () in
    let update = Update.Delete key in
    let update_stats () = Statistics.new_delete _stats start in
    self # _update_rendezvous update update_stats push_update

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
		Lwt.return start_i
	    end
	    >>= fun start_i2->
	    let step = Sn.of_int (!Tlogcommon.tlogEntriesPerFile) in
	    let rec loop_parts (start_i2:Sn.t) =
	      if Sn.rem start_i2 step = Sn.start &&
		 Sn.add start_i2 step < too_far_i 
	      then
		begin
		  Lwt_log.debug_f "start_i2=%Li < %Li" start_i2 too_far_i 
		  >>= fun () ->
		  Llio.output_int oc 3 >>= fun () ->
		  tlog_collection # dump_tlog_file start_i2 oc 
		  >>= fun start_i2' ->
		  loop_parts start_i2'
		end
	      else 
		Lwt.return start_i2
	    in
	    loop_parts start_i2
	    >>= fun start_i3 ->
	    Llio.output_int oc 1 >>= fun () ->
	    let f(i,u) = Tlogcommon.write_entry oc i u in
	    tlog_collection # iterate start_i3 too_far_i f >>= fun () ->
	    Sn.output_sn oc (-1L) 
	  end
    end
    >>= fun () ->
    log_o self "done with_last_entries"


  method sequence (updates:Update.t list) =
    let start = Unix.gettimeofday() in
    log_o self "sequence" >>= fun () ->
    let update = Update.Sequence updates in
    let update_stats () = Statistics.new_sequence _stats start in
    self # _update_rendezvous update update_stats push_update

  method multi_get ~allow_dirty (keys:string list) =
    let start = Unix.gettimeofday() in
    log_o self "multi_get" >>= fun () ->
    self # _read_allowed allow_dirty >>= fun () ->
    store # multi_get keys >>= fun values ->
    Statistics.new_multiget _stats start;
    Lwt.return values

  method to_string () = "sync_backend(" ^ (Node_cfg.node_name cfg) ^")"

  method private _who_master () =
    store # who_master ()

  method who_master () =
    self # _who_master () >>= fun mo ->
    let result,argumentation = 
      match mo with
	| None -> None,"young cluster"
	| Some (m,ls) ->
	  match Node_cfg.get_master cfg with
	    | Elected | Preferred _ ->
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
	    | Forced x -> Some x,"forced master"
	    | ReadOnly -> Some my_name, "readonly"

    in
    log_o self "master:%s (%s)" (string_option_to_string result) argumentation 
    >>= fun () ->
    Lwt.return result

  method private _not_if_master() =
    self # who_master () >>= function
      | None ->
        Lwt.return () 
      | Some m ->
        if m = my_name
        then
          Lwt.fail (XException(Arakoon_exc.E_UNKNOWN_FAILURE, "Operation cannot be performed on master node"))
        else
          Lwt.return ()
      
  method private _write_allowed () =
    if read_only 
    then Lwt.fail (XException(Arakoon_exc.E_READ_ONLY, my_name))
    else
      begin
	self # who_master () >>= function
	  | None ->
	    Lwt.fail (XException(Arakoon_exc.E_NOT_MASTER, "None"))
	  | Some m ->
	    if m <> my_name
	    then
	      Lwt.fail (XException(Arakoon_exc.E_NOT_MASTER, m))
	    else
	      Lwt.return ()
      end
	
  method private _read_allowed (allow_dirty:bool) =
    if allow_dirty or read_only
    then Lwt.return ()
    else self # _write_allowed ()

  method private _check_interval keys = 
    store # get_interval () >>= fun iv ->
    let rec loop = function
      | [] -> Lwt.return ()
     (* | [k] -> 
	if Interval.is_ok iv k then Lwt.return ()
	else Lwt.fail (XException(Arakoon_ex.E_OUTSIDE_INTERVAL, k)) *)
      | k :: keys -> 
	if Interval.is_ok iv k 
	then loop keys
	else Lwt.fail (XException(Arakoon_exc.E_OUTSIDE_INTERVAL, k))
    in
    loop keys

  method private _check_interval_range first last =
    store # get_interval () >>= fun iv ->
    let check_option = function
      | None -> Lwt.return () 
      | Some k -> 
	if Interval.is_ok iv k
	then Lwt.return ()
	else Lwt.fail (XException(Arakoon_exc.E_OUTSIDE_INTERVAL, k))
    in
    check_option first >>= fun () ->
    check_option last  

  method witness name i = 
    Statistics.witness _stats name i;
    Lwt_log.debug_f "witnessed (%s,%s)" name (Sn.string_of i) >>= fun () ->
    store # consensus_i () >>= fun cio ->
    begin
      match cio with
	| None -> ()
	| Some ci -> Statistics.witness _stats my_name  ci
    end;
    Lwt.return ()
      
  method last_witnessed name = Statistics.last_witnessed _stats name
    
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
	  ( Statistics.get_witnessed _stats ) (1,"") in
	let v = count >= q in
	Lwt_log.debug_f "count:%i,q=%i i=%s detail:%s" count q (Sn.string_of i) s
	>>= fun () ->
	Lwt.return v

  method get_statistics () = _stats

  method check ~cluster_id = 
    let r = test ~cluster_id in
    Lwt.return r

  method collapse n cb' cb = 
    begin
      if n < 1 then 
        let rc = Arakoon_exc.E_UNKNOWN_FAILURE
        and msg = Printf.sprintf "%i is not acceptable" n
        in
        Lwt.fail (XException(rc,msg))
      else 
        Lwt_log.debug_f "collapsing_lock locked: %s"
          (string_of_bool (Lwt_mutex.is_locked collapsing_lock)) >>= fun () ->
        if Lwt_mutex.is_locked collapsing_lock then
	        let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	        and msg = "Collapsing already in progress"
          in
          Lwt.fail (XException(rc,msg))
        else
          Lwt.return ()
    end >>= fun () ->
    Lwt_mutex.with_lock collapsing_lock (fun () ->
      let new_cb tlog_num =
        cb() >>= fun () ->
        self # wait_for_tlog_release tlog_num
      in
      Collapser.collapse_many tlog_collection store_methods n cb' new_cb 
    )
    
  method get_routing () =
    self # _read_allowed false >>= fun () -> 
    Lwt.catch
      (fun () ->
        store # get_routing ()  
      )
      (fun exc ->
        match exc with
          | Store.CorruptStore ->
            begin
              Lwt_log.fatal "CORRUPT_STORE" >>= fun () ->
              Lwt.fail Server.FOOBAR
            end
          | ext -> Lwt.fail ext)   


  method get_key_count () =
    store # get_key_count ()  
    
  method get_db m_oc =
    self # _not_if_master () >>= fun () ->
    Lwt_log.debug "get_db: enter" >>= fun () ->
    let result = ref Multi_paxos.Quiesced_fail in
    begin
      match m_oc with
        | None -> 
          let ex = XException(Arakoon_exc.E_UNKNOWN_FAILURE, "Can only stream on a valid out channel") in
          Lwt.fail ex
        | Some oc -> Lwt.return oc
    end >>= fun oc ->
    let sleep, awake = Lwt.wait() in
    let update = Multi_paxos.Quiesce (sleep, awake) in
    Lwt_log.debug "get_db: Pushing quiesce request" >>= fun () ->
    push_node_msg update >>= fun () ->
    Lwt.finalize 
    ( fun () ->
	    begin
	      Lwt_log.debug "get_db: waiting for quiesce request to be completed" >>= fun () ->
	      sleep >>= fun res ->
	      result := res;
	      match res with
	        | Multi_paxos.Quiesced_ok -> store # copy_store oc
	        | Multi_paxos.Quiesced_fail_master ->
	          Lwt.fail (XException(Arakoon_exc.E_UNKNOWN_FAILURE, "Operation cannot be performed on master node"))
	        | Multi_paxos.Quiesced_fail ->
	          Lwt.fail (XException(Arakoon_exc.E_UNKNOWN_FAILURE, "Store could not be quiesced"))
	    end
    )
    ( fun () ->
      begin
        let res = !result in
        begin
	        match res with 
	          | Multi_paxos.Quiesced_ok ->
	                Lwt_log.debug "get_db: Leaving quisced state" >>= fun () ->
	                let update = Multi_paxos.Unquiesce in
	                push_node_msg update 
	          | _ -> Lwt.return ()
        end >>= fun () ->
        Lwt_log.debug "get_db: All done"
      end
    )
  
  method get_cluster_cfgs () =
    begin
      match client_cfgs with 
        | None ->
          store # range_entries ~_pf:__adminprefix 
            ncfg_prefix_b4_o false ncfg_prefix_2far_o false (-1) 
          >>= fun cfgs ->
          let result = Hashtbl.create 5 in
          let add_item (item: string*string) = 
            let (k,v) = item in
            let cfg, _ = ClientCfg.cfg_from v 0 in
            let start = String.length ncfg_prefix_b4 in
            let length = (String.length k) - start in
            let k' = String.sub k start length in 
            Hashtbl.replace result k' cfg
          in
          List.iter add_item cfgs;
          let () = client_cfgs <- (Some result) in 
          Lwt.return result
        | Some res ->
          Lwt.return res
    end
        
  method set_cluster_cfg cluster_id cfg =
    let key = ncfg_prefix_b4 ^ cluster_id in
    let buf = Buffer.create 100 in
    ClientCfg.cfg_to buf cfg;
    let value = Buffer.contents buf in
    let update = Update.AdminSet (key,Some value) in
    self # _update_rendezvous update (fun () -> ()) push_update >>= fun () ->
    begin
      match client_cfgs with
        | None ->
          let res = Hashtbl.create 5 in
          Hashtbl.replace res cluster_id cfg;
          Lwt_log.debug "set_cluster_cfg creating new cached hashtbl" >>= fun () ->
          Lwt.return ( client_cfgs <- (Some res) )
        | Some res -> 
          Lwt_log.debug "set_cluster_cfg updating cached hashtbl" >>= fun () ->
          Lwt.return ( Hashtbl.replace res cluster_id cfg ) 
    end
    
  method get_fringe boundary direction = 
    Lwt_log.debug_f "get_fringe %S" boundary >>= fun () ->
    store # get_fringe boundary direction 
end
