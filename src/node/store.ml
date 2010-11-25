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
open Lwt
open Log_extra


let __i_key = "*i"
let __master_key  = "*master"
let __lease_key = "*lease"
let __prefix = "@"

(** common interface for stores *)
class type store = object
  method exists: string -> bool Lwt.t
  method get: string -> string Lwt.t
  method range: string option -> bool -> string option -> bool -> int -> string list Lwt.t
  method range_entries: string option -> bool -> string option -> bool -> int -> (string * string) list Lwt.t
  method prefix_keys: string -> int -> string list Lwt.t
  method set: string -> string -> unit Lwt.t
  method test_and_set: string -> string option -> string option -> string option Lwt.t
  method delete: string -> unit Lwt.t
  method sequence : Update.t list -> unit Lwt.t
  method set_master: string -> int64 -> unit Lwt.t
  method set_master_no_inc: string -> int64 -> unit Lwt.t
  method who_master: unit -> (string*int64) option Lwt.t
    
  (** last value on which there is consensus.
      For an empty store, This is None 
  *)
  method consensus_i: unit -> Sn.t option Lwt.t


  method close: unit -> unit Lwt.t
end

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
    | Update.TestAndSet(key,expected,wanted)->
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
    | Update.Sequence updates ->
      with_error "Not_found" (fun () -> store # sequence updates)
    | Update.Nop -> Lwt.return (Ok None)

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
  _insert_update store update

let _insert (store:store) v i =
  let Value.V(update_string) = v in
  let u,_ = Update.from_buffer update_string 0 in
  _insert_update store u

let _insert (store:store) v i =
  let Value.V(update_string) = v in
  let u,_ = Update.from_buffer update_string 0 in
  _insert_update store u

let on_consensus (store:store) (v,n,i) =
  Lwt_log.debug_f "on_consensus=> local_store %s %s %s" 
    (Value.string_of v) (Sn.string_of n) (Sn.string_of i)
  >>= fun () ->
  _insert store v i >>= fun maybe_result ->
  Lwt.return maybe_result

exception TrailingStore of (Sn.t option * Sn.t option)

let verify (store:store) tlog_i =
  store#consensus_i () >>= fun store_i ->
  begin
    let store_is = option_to_string Sn.string_of store_i in
    let tlog_is = option_to_string Sn.string_of tlog_i in
    let new_sn,case =
      match tlog_i, store_i with
	| None ,None -> Sn.start,0
	| Some 0L, None -> Sn.start,1
	| Some i, Some j when i = Sn.succ j ->
	  Sn.succ j,2
	| Some i, Some j when i = j -> Sn.succ j,3
	| a,b ->
	  let exn = TrailingStore (a,b) in
	  raise exn
    in
    Lwt_log.info_f "VERIFY TLOG & STORE %s %s OK!" tlog_is store_is
    >>= fun () ->
    Lwt.return (new_sn,case )
  end
