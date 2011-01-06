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
open Clone

let _s_ = function
  | Some x -> "Some " ^ x
  | None -> "None"

exception Forced_stop

class sync_backend cfg push_update 
  (store:Store.store) 
  (tlog_collection:Tlogcollection.tlog_collection) 
  (lease_expiration:int)
  quorum_function n_nodes
  =
  let my_name =  Node_cfg.node_name cfg in
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
	  | ext -> Lwt.fail ext)

  method range (first:string option) finc (last:string option) linc max =
    log_o self "%s %b %s %b %i" (_s_ first) finc (_s_ last) linc max >>= fun () ->
    self # _only_if_master () >>= fun () ->
    store # range first finc last linc max

  method range_entries (first:string option) finc (last:string option) linc max =
    log_o self "%s %b %s %b %i" (_s_ first) finc (_s_ last) linc max >>= fun () ->
    self # _only_if_master () >>= fun () ->
    store # range_entries first finc last linc max

  method prefix_keys (prefix:string) (max:int) =
    log_o self "prefix_keys %s %d" prefix max >>= fun () ->
    self # _only_if_master () >>= fun () ->
    store # prefix_keys prefix max

  method set key value =
    log_o self "set %S %S" key value >>= fun () ->
    self # _only_if_master () >>= fun () ->
    let update = Update.Set(key,value) in
    let p_value = Update.make_update_value update in
    let sleep, awake = Lwt.wait () in
    let went_well b =
      begin
	Lwt_log.debug "went_well" >>= fun () ->
	Lwt.wakeup awake b;
	let () = Statistics.new_set _stats key value in
	Lwt.return ()
      end
    in push_update (Some p_value, went_well) >>= fun () ->
    sleep >>= function
      | Store.Stop -> Lwt.fail Forced_stop
      | Store.Update_fail (rc,str) -> Lwt.fail (XException(rc,str))
      | Store.Ok _ -> Lwt.return ()

  method test_and_set key expected (wanted:string option) =
    log_o self "test_and_set %s %s %s" key 
      (string_option_to_string expected) 
      (string_option_to_string wanted)
    >>= fun () ->
    self # _only_if_master () >>= fun () ->
    let update = Update.TestAndSet(key, expected, wanted) in
    let p_value = Update.make_update_value update in
    let sleep, awake = Lwt.wait () in
    let went_well b =
      begin
	Lwt_log.debug_f "went_well" >>= fun () ->
	Lwt.wakeup awake b;
	Lwt.return ()
      end
    in push_update (Some p_value, went_well) >>= fun () ->
    sleep >>= function
      | Store.Stop -> Lwt.fail Forced_stop
      | Store.Update_fail (rc,str) -> Lwt.fail (Failure str)
      | Store.Ok x -> Lwt.return x

  method delete key = log_o self "delete %S" key >>= fun () ->
    self # _only_if_master ()>>= fun () ->
    let update = Update.Delete key in
    let p_value = Update.make_update_value update in
    let sleep, awake = Lwt.wait() in
    let went_well b =
      begin
	Lwt_log.debug_f "went_well" >>= fun () ->
	Lwt.wakeup awake b;
	Statistics.new_delete _stats;
	Lwt.return ()
      end
    in push_update (Some p_value, went_well) >>= fun () ->
    sleep >>= function
      | Store.Stop -> Lwt.fail Forced_stop
      | Store.Update_fail (rc,str) -> Lwt.fail (XException(rc, str))
      | Store.Ok _ -> Lwt.return ()

  method hello (client_info:string) =
    log_o self "hello" >>= fun () -> Lwt.return "xxx"

  method last_entries (start_i:Sn.t) f =
    log_o self "last_entries %s" (Sn.string_of start_i) >>= fun () ->
    store # consensus_i () >>= fun consensus_i ->
    begin
      match consensus_i with
	| None -> Lwt.return () 
	| Some ci -> 
	  begin
	    log_o self "consensus_i = %s" (Sn.string_of ci) >>= fun () ->
	    tlog_collection # iterate start_i ci f 
	  end
    end 
    >>= fun () ->
    log_o self "done with_last_entries"


  method sequence (updates:Update.t list) =
    log_o self "sequence" >>= fun () ->
    let update = Update.Sequence updates in
    let p_value = Update.make_update_value update in
    let sleep, awake = Lwt.wait () in
    let went_well b =
      begin
	Lwt_log.debug_f "went_well" >>= fun () ->
	Lwt.wakeup awake b;
	Statistics.new_sequence _stats;
	Lwt.return ()
      end
    in push_update (Some p_value, went_well) >>= fun () ->
    sleep >>= function
      | Store.Stop -> Lwt.fail Forced_stop
      | Store.Update_fail (rc, str) -> Lwt.fail (XException(rc, str))
      | Store.Ok _ -> Lwt.return ()

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
	      then None,"invalid lease from previous incarnation"
	      else
		let now = Int64.of_float (Unix.time()) in
		let diff = Int64.sub now ls in
		if diff < Int64.of_int lease_expiration then
		  (Some m,"inside lease")
		else (None,"lease expired")
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
    Lwt.return ()

  method expect_progress_possible () = 
    store # consensus_i () >>= function 
      | None -> Lwt.return false
      | Some i ->
	let q = quorum_function n_nodes in
	let count,s = Hashtbl.fold
	  (fun name ci (count,s) -> 
	    let s' = s ^ Printf.sprintf " (%s,%s) " name (Sn.string_of ci) in
	    if ci = i 
	    then  count+1,s' 
	    else  count,s') 
	  witnessed (1,"") in
	let v = 

	  if count >= q
	  then true
	  else false
	in 
	Lwt_log.debug_f "count:%i,q=%i i=%s detail:%s" count q (Sn.string_of i) s
	>>= fun () ->
	Lwt.return v

  method get_statistics () = _stats

  method clone (ic,oc) = Clone.send_files (ic,oc) cfg.Node_cfg.home
end
