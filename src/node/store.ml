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

open Update
open Interval
open Routing
open Lwt
open Log_extra

let __i_key = "*i"
let __interval_key = "*interval"
let __routing_key = "*routing"
let __master_key  = "*master"
let __lease_key = "*lease"
let __prefix = "@"
let __adminprefix="*"

let _f _pf = function
  | Some x -> (Some (_pf ^ x))
  | None -> (Some _pf)
let _l _pf = function
  | Some x -> (Some (_pf ^ x))
  | None -> None

let _filter pf =
  let pl = String.length pf in
  Array.fold_left (fun acc key ->
    let kl = String.length key in
    let key' = String.sub key pl (kl-pl) in
    key'::acc) []


(** common interface for stores *)
class type store = object
  method exists: ?_pf: string -> string -> bool Lwt.t
  method get: ?_pf: string -> string -> string Lwt.t
  method multi_get: ?_pf: string -> string list -> string list Lwt.t
  method range: ?_pf: string -> string option -> bool -> string option -> bool -> int -> string list Lwt.t
  method range_entries: ?_pf: string -> string option -> bool -> string option -> bool -> int -> (string * string) list Lwt.t
  method rev_range_entries: ?_pf: string -> string option -> bool -> string option -> bool -> int -> (string * string) list Lwt.t
  method prefix_keys: ?_pf: string -> string -> int -> string list Lwt.t
  method set: ?_pf: string -> string -> string -> unit Lwt.t
  method test_and_set: ?_pf: string -> string -> string option -> string option -> string option Lwt.t
  method delete: ?_pf: string -> string -> unit Lwt.t
  method delete_prefix: ?_pf: string -> string -> int Lwt.t
  method sequence : ?_pf: string -> Update.t list -> unit Lwt.t
  method set_master: string -> int64 -> unit Lwt.t
  method set_master_no_inc: string -> int64 -> unit Lwt.t
  method who_master: unit -> (string*int64) option Lwt.t

  (** last value on which there is consensus.
      For an empty store, This is None
  *)
  method consensus_i: unit -> Sn.t option Lwt.t
  method incr_i: unit -> unit Lwt.t
  method close: unit -> unit Lwt.t
  method reopen: (unit -> unit Lwt.t) -> unit Lwt.t

  method get_location: unit -> string
  method relocate: string -> unit Lwt.t

  method aSSert: ?_pf: string -> string -> string option -> bool Lwt.t

  method user_function : string -> string option -> (string option) Lwt.t
  method get_interval: unit -> Interval.t Lwt.t
  method set_interval: Interval.t -> unit Lwt.t
  method get_routing : unit -> Routing.t Lwt.t
  method set_routing : Routing.t -> unit Lwt.t
  method set_routing_delta: string -> string -> string -> unit Lwt.t

  method get_key_count : ?_pf: string -> unit -> int64 Lwt.t

  method quiesce : unit -> unit Lwt.t
  method unquiesce : unit -> unit Lwt.t
  method quiesced : unit -> bool
  method optimize : unit -> unit Lwt.t
  method defrag : unit -> unit Lwt.t
  method copy_store : ?_networkClient: bool -> Lwt_io.output_channel -> unit Lwt.t
  method get_fringe : string option -> Routing.range_direction -> (string * string) list Lwt.t

end

exception Key_not_found of string ;;
exception CorruptStore

type update_result =
  | Stop
  | Ok of string option
  | Update_fail of Arakoon_exc.rc * string

let _insert_update (store:store) update =
  let with_error notfound_msg f =
    Lwt.catch
      (fun () ->
	f () >>= fun () -> Lwt.return (Ok None))
      (function
	| Not_found ->
	  let rc = Arakoon_exc.E_NOT_FOUND
	  and msg = notfound_msg in
	  Lwt.return (Update_fail(rc, msg))
    | Arakoon_exc.Exception(rc,msg) -> Lwt.return (Update_fail(rc,msg))
	| e ->
	  let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	  and msg = Printexc.to_string e in
	  Lwt.return (Update_fail(rc, msg))
      )
  in
  match update with
    | Update.Set(key,value) ->
      with_error key (fun () -> store # set key value)
    | Update.MasterSet (m, lease) ->
      with_error "Not_found" (fun () -> store # set_master m lease)
    | Update.Delete(key) ->
      with_error key (fun () -> store # delete key)
    | Update.DeletePrefix prefix ->
      begin
        Lwt.catch
          (fun () ->
            store # delete_prefix prefix >>= fun n_deleted ->
            let sb = Buffer.create 8 in
            let () = Llio.int_to sb n_deleted in
            let ser = Buffer.contents sb in
            Lwt.return (Ok (Some ser)))
          (fun e -> 
            let rc = Arakoon_exc.E_UNKNOWN_FAILURE
            and msg = Printexc.to_string e
            in
            Lwt.return (Update_fail (rc,msg)))
      end
    | Update.TestAndSet(key,expected,wanted)->
      begin
        Lwt.catch
	      (fun () ->
	        store # test_and_set key expected wanted >>= fun res ->
	        Lwt.return (Ok res))
	      (function
	        | Not_found ->
	          let rc = Arakoon_exc.E_NOT_FOUND
	          and msg = key in
	          Lwt.return (Update_fail (rc,msg))
	        | e ->
	          let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	          and msg = Printexc.to_string e
	          in
	          Lwt.return (Update_fail (rc,msg))
	      )
      end
    | Update.UserFunction(name,po) ->
      Lwt.catch
	    (fun () ->
	      store # user_function name po >>= fun ro ->
	      Lwt.return (Ok ro)
	    )
	    (function
	      | Common.XException(rc,msg) -> Lwt.return (Update_fail(rc,msg))
	      | e ->
	        let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	        and msg = Printexc.to_string e
	        in
	        Lwt.return (Update_fail(rc,msg))
	    )
    | Update.Sequence updates 
    | Update.SyncedSequence updates ->
      Lwt.catch
        (fun () ->
          store # sequence updates >>= fun () ->
          Lwt.return (Ok None))
        (function
          | Key_not_found key ->
            let rc = Arakoon_exc.E_NOT_FOUND
            and msg = key in
            Lwt.return (Update_fail (rc,msg))
          | Not_found ->
            let rc = Arakoon_exc.E_NOT_FOUND
            and msg = "Not_found" in
            Lwt.return (Update_fail (rc,msg))
	  | Arakoon_exc.Exception(rc,msg) ->
	    Lwt.return (Update_fail(rc,msg))
          | e ->
            let rc = Arakoon_exc.E_UNKNOWN_FAILURE
            and msg = Printexc.to_string e
            in
            Lwt.return (Update_fail (rc,msg))
        )
    | Update.SetInterval interval ->
      Lwt.catch
	(fun () ->
	  store # set_interval interval >>= fun () ->
	  Lwt.return (Ok None))
	(function
	  | Common.XException (rc,msg) -> Lwt.return (Update_fail(rc,msg))
	  | e ->
	    let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	    and msg = Printexc.to_string e
	    in
	    Lwt.return (Update_fail (rc,msg)))
    | Update.SetRouting routing ->
      Lwt.catch
	(fun () ->
	  store # set_routing routing >>= fun () ->
	  Lwt.return (Ok None))
	(function
	  | Common.XException (rc, msg) -> Lwt.return (Update_fail(rc,msg))
	  | e ->
	    let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	    and msg = Printexc.to_string e
	    in
	    Lwt.return (Update_fail (rc,msg))
	)
    | Update.SetRoutingDelta (left, sep, right) ->
      Lwt.catch
    (fun () ->
      store # set_routing_delta left sep right >>= fun () ->
      Lwt.return (Ok None))
    (function
      | Common.XException (rc, msg) -> Lwt.return (Update_fail(rc,msg))
      | e ->
        let rc = Arakoon_exc.E_UNKNOWN_FAILURE
        and msg = Printexc.to_string e
        in
        Lwt.return (Update_fail (rc,msg))
    )
    | Update.Nop -> Lwt.return (Ok None)
    | Update.Assert(k,vo) ->
      Lwt.catch
	(fun () -> store # aSSert k vo >>= function
	  | true -> Lwt.return (Ok None)
	  | false -> Lwt.return (Update_fail(Arakoon_exc.E_ASSERTION_FAILED,k))
	)
	(fun e ->
	  let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	  and msg = Printexc.to_string e
	  in Lwt.return (Update_fail(rc, msg)))
    | Update.AdminSet(k,vo) ->
      Lwt.catch(
        fun () ->
          begin
            match vo with
            | None ->
              store # delete ~_pf:__adminprefix k
            | Some v ->
              store # set ~_pf:__adminprefix k v
          end
          >>= fun () ->
          Lwt.return (Ok None)
      ) (
        fun e ->
          let rc = Arakoon_exc.E_UNKNOWN_FAILURE
          and msg = Printexc.to_string e
          in Lwt.return (Update_fail(rc,msg))
      )


let safe_insert_update (store:store) (i:Sn.t) update =
  (* TODO: race condition:
     change between lookup of i and insert...
     rethink transaction wrapping strategy
  *)
  store # consensus_i () >>= fun store_i ->
  begin
    match i, store_i with
      | 0L , None -> Lwt.return ()
      | n, None -> Llio.lwt_failfmt "store is empty, update @ %s" (Sn.string_of n)
      | n, Some m ->
	if n = Sn.succ m
	then Lwt.return ()
	else Llio.lwt_failfmt "update %s, store @ %s don't fit" (Sn.string_of n) (Sn.string_of m)
  end
  >>= fun () ->
  if store # quiesced ()
  then
    begin
      store # incr_i () >>= fun () ->
      Lwt.return (Ok None)
    end
  else
    begin
      _insert_update store update
    end

let _insert (store:store) v i =
  let Value.V(update_string) = v in
  let u,_ = Update.from_buffer update_string 0 in
  _insert_update store u

let on_consensus (store:store) (v,n,i) =
  Lwt_log.debug_f "on_consensus=> local_store n=%s i=%s"
    (Sn.string_of n) (Sn.string_of i)
  >>= fun () ->
  store # consensus_i () >>= fun m_store_i ->
  begin
  match m_store_i with
  | None ->
    if Sn.compare i Sn.start == 0
    then
      Lwt.return()
    else
      Llio.lwt_failfmt "Invalid update to empty store requested (%s)" (Sn.string_of i)
  | Some store_i ->
    if (Sn.compare (Sn.pred i) store_i) == 0
    then
      Lwt.return()
    else
      Llio.lwt_failfmt "Invalid store update requested (%s : %s)"
	(Sn.string_of i) (Sn.string_of store_i)
  end >>= fun () ->
  if store # quiesced () then
    begin
      store # incr_i () >>= fun () ->
      Lwt.return (Ok None)
    end
  else
    begin
      _insert store v i >>= fun maybe_result ->
      Lwt.return maybe_result
    end

let get_succ_store_i (store:store) =
  store # consensus_i () >>= fun m_si ->
  match m_si with
    | None -> Lwt.return Sn.start
    | Some si -> Lwt.return (Sn.succ si)

let get_catchup_start_i = get_succ_store_i
