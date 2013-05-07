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
let __j_key = "*j"
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

class transaction = object end
class transaction_lock = object end

(** common interface for stores *)
class type store = object
  method with_transaction_lock: (transaction_lock -> 'a Lwt.t) -> 'a Lwt.t
  method with_transaction: ?key: transaction_lock option -> (transaction -> 'a Lwt.t) -> 'a Lwt.t

  method exists: string -> bool Lwt.t
  method get: string -> string

  method range: ?_pf: string -> string option -> bool -> string option -> bool -> int -> string list Lwt.t
  method range_entries: ?_pf: string -> string option -> bool -> string option -> bool -> int -> (string * string) list Lwt.t
  method rev_range_entries: ?_pf: string -> string option -> bool -> string option -> bool -> int -> (string * string) list Lwt.t
  method prefix_keys: ?_pf: string -> string -> int -> string list Lwt.t
  method set: transaction -> string -> string -> unit
  method delete: transaction -> string -> unit
  method delete_prefix: transaction -> ?_pf: string -> string -> int Lwt.t
  method set_master: transaction -> string -> int64 -> unit Lwt.t
  method set_master_no_inc: string -> int64 -> unit Lwt.t
  method who_master: unit -> (string*int64) option

  (** last value on which there is consensus.
      For an empty store, This is None
  *)
  method consensus_i: unit -> Sn.t option
  method incr_i: transaction -> unit Lwt.t
  method is_closed : unit -> bool
  method close: unit -> unit Lwt.t
  method reopen: (unit -> unit Lwt.t) -> unit Lwt.t

  method get_location: unit -> string
  method relocate: string -> unit Lwt.t

  method aSSert: transaction -> ?_pf: string -> string -> string option -> bool Lwt.t
  method aSSert_exists: transaction -> ?_pf: string -> string           -> bool Lwt.t

  method user_function : transaction -> string -> string option -> (string option) Lwt.t
  method get_interval: unit -> Interval.t Lwt.t
  method set_interval: transaction -> Interval.t -> unit Lwt.t
  method get_routing : unit -> Routing.t Lwt.t
  method set_routing : transaction -> Routing.t -> unit Lwt.t
  method set_routing_delta: transaction -> string -> string -> string -> unit Lwt.t

  method get_key_count : ?_pf: string -> unit -> int64 Lwt.t

  method quiesce : unit -> unit Lwt.t
  method unquiesce : unit -> unit Lwt.t
  method quiesced : unit -> bool
  method optimize : unit -> unit Lwt.t
  method defrag : unit -> unit Lwt.t
  method copy_store : ?_networkClient: bool -> Lwt_io.output_channel -> unit Lwt.t
  method get_fringe : string option -> Routing.range_direction -> (string * string) list Lwt.t

end

exception Key_not_found of string 
exception CorruptStore

let get (store:store) key =
  try
    Lwt.return (store # get (__prefix ^ key))
  with
    | Failure msg ->
        let _closed = store # is_closed () in
        Lwt_log.debug_f "local_store: Failure %Swhile GET (_closed:%b)" msg _closed >>= fun () ->
        if _closed
        then
          Lwt.fail (Common.XException (Arakoon_exc.E_GOING_DOWN,
                                       Printf.sprintf "GET %S database already closed" key))
        else
          Lwt.fail CorruptStore
	| exn -> Lwt.fail exn

let get_option (store:store) key =
  try
    Some (store # get (__prefix ^ key))
  with Not_found -> None

let exists (store:store) key =
  store # exists (__prefix ^ key)

let multi_get (store:store) keys =
  let vs = List.fold_left (fun acc key ->
    try
      let v = store # get key in
      v::acc
    with Not_found ->
      let exn = Common.XException(Arakoon_exc.E_NOT_FOUND, key) in
      raise exn)
    [] keys
  in
  Lwt.return (List.rev vs)

let multi_get_option (store:store) keys =
  let vs = List.fold_left (fun acc key ->
    try
      let v = store # get key in
      (Some v)::acc
    with Not_found -> None::acc)
    [] keys
  in
  Lwt.return (List.rev vs)

let get_j (store:store) =
  try
    let jstring = store # get __j_key in
    int_of_string jstring
  with Not_found -> 0

let set_j (store:store) tx j =
  store # set tx __j_key (string_of_int j)

type update_result =
  | Stop 
  | Ok of string option
  | Update_fail of Arakoon_exc.rc * string

type key_or_transaction =
  | Key of transaction_lock
  | Transaction of transaction

let with_transaction : store -> key_or_transaction -> (transaction -> 'a Lwt.t) -> 'a Lwt.t =
  fun store kt f -> match kt with
    | Key key -> store # with_transaction ~key:(Some key) f
    | Transaction tx -> f tx

let _insert_update (store:store) (update:Update.t) kt =
  let with_transaction f = with_transaction store kt f in
  let catch_with_tx tx f g =
    Lwt.catch
      (fun () ->
        let j = get_j store in
        Lwt.finalize
          (fun () -> f tx)
          (fun () -> Lwt.return (set_j store tx (j + 1))))
      g in
  let with_error tx notfound_msg f =
    catch_with_tx tx
      (fun tx -> f tx >>= fun () -> Lwt.return (Ok None))
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
  let rec do_one update tx =
    match update with
      | Update.Set(key,value) ->
          with_error tx key (fun tx -> Lwt.return (store # set tx (__prefix ^ key) value))
      | Update.MasterSet (m, lease) ->
          with_error tx "Not_found" (fun tx -> store # set_master tx m lease)
      | Update.Delete(key) ->
          with_error tx key (fun tx -> Lwt.return (store # delete tx (__prefix ^ key)))
      | Update.DeletePrefix prefix ->
          begin
            catch_with_tx tx
              (fun tx ->
                store # delete_prefix tx prefix >>= fun n_deleted ->
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
            catch_with_tx tx
              (fun tx ->
                let existing = get_option store key in
                if existing <> expected
                then
                  Lwt.return (Ok existing)
                else
                  begin
                    let () = match wanted with
                      | None -> store # delete tx (__prefix ^ key)
                      | Some wanted_s -> store # set tx (__prefix ^ key) wanted_s in
                    Lwt.return (Ok wanted)
                  end)
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
          catch_with_tx tx
            (fun tx ->
              store # user_function tx name po >>= fun ro ->
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
          catch_with_tx tx
            (fun tx ->
              Lwt_list.iter_s (fun update -> do_one update tx >>=
                fun r -> match r with
                  | Ok _ -> Lwt.return ()
                      (* TODO correct exception, match Stop *)
                  | Update_fail (rc, msg) -> failwith msg) updates >>= fun () ->
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
          catch_with_tx tx
            (fun tx ->
              store # set_interval tx interval >>= fun () ->
              Lwt.return (Ok None))
            (function
              | Common.XException (rc,msg) -> Lwt.return (Update_fail(rc,msg))
              | e ->
                  let rc = Arakoon_exc.E_UNKNOWN_FAILURE
                  and msg = Printexc.to_string e
                  in
                  Lwt.return (Update_fail (rc,msg)))
      | Update.SetRouting routing ->
          catch_with_tx tx
            (fun tx ->
              store # set_routing tx routing >>= fun () ->
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
          catch_with_tx tx
            (fun tx ->
              store # set_routing_delta tx left sep right >>= fun () ->
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
          catch_with_tx tx
	        (fun tx -> store # aSSert tx k vo >>= function
	          | true -> Lwt.return (Ok None)
	          | false -> Lwt.return (Update_fail(Arakoon_exc.E_ASSERTION_FAILED,k))
	        )
	        (fun e ->
	          let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	          and msg = Printexc.to_string e
	          in Lwt.return (Update_fail(rc, msg)))
      | Update.Assert_exists(k) ->
          catch_with_tx tx
	        (fun tx -> store # aSSert_exists tx k >>= function
	          | true -> Lwt.return (Ok None)
	          | false -> Lwt.return (Update_fail(Arakoon_exc.E_ASSERTION_FAILED,k))
	        )
	        (fun e ->
	          let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	          and msg = Printexc.to_string e
	          in Lwt.return (Update_fail(rc, msg)))
      | Update.AdminSet(k,vo) ->
          catch_with_tx tx
            (fun tx ->
              let () =
                match vo with
                  | None   -> store # delete tx (__adminprefix ^ k)
                  | Some v -> store # set    tx (__adminprefix ^ k) v
              in
              Lwt.return (Ok None)
            )
            (fun e ->
              let rc = Arakoon_exc.E_UNKNOWN_FAILURE
              and msg = Printexc.to_string e
              in Lwt.return (Update_fail(rc,msg))
            )
  in
  let get_key = function
    | Update.Set (key,value) -> Some key
    | Update.Delete key -> Some key
    | Update.TestAndSet (key, expected, wanted) -> Some key
    | _ -> None
  in
  try
    with_transaction (do_one update)
  with
    | Not_found ->
        let key = get_key update
        in match key with
          | Some key -> raise (Key_not_found key)
          | None -> raise Not_found

let _insert_updates (store:store) (us: Update.t list) kt =
  let f u = _insert_update store u kt in
  Lwt_list.map_s f us

let _insert_value (store:store) (value:Value.t) kt =
  let updates = Value.updates_from_value value in
  let j = get_j store in
  let skip n l =
    let rec inner = function
      | 0, l -> l
      | n, [] -> failwith "need to skip more updates than present in this paxos value"
      | n, hd::tl -> inner ((n - 1), tl) in
    inner (n, l) in
  let updates' = skip j updates in
  _insert_updates store updates' kt >>= fun (urs:update_result list) ->
  with_transaction store kt (fun tx ->
    store # incr_i tx >>= fun () ->
    Lwt.return (set_j store tx 0)) >>= fun () ->
  Lwt.return urs


let safe_insert_value (store:store) ?(tx=None) (i:Sn.t) value =
  let inner f =
    match tx with
      | None ->   store # with_transaction_lock (fun key -> f (Key key))
      | Some tx -> f (Transaction tx) in
  let t kt =
    let store_i = store # consensus_i () in
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
        with_transaction store kt (fun tx -> store # incr_i tx) >>= fun () ->
        Lwt.return [Ok None]
      end
    else
      begin
        let results = _insert_value store value kt in
        match tx with
          | None -> results
          | Some _ ->
              results >>= fun results ->
              List.iter (fun result -> match result with
                | Ok _ -> ()
                | _ -> failwith "some updates failed to apply to the store") results;
              Lwt.return results
      end
  in
  inner t



let on_consensus (store:store) (v,n,i) =
  Lwt_log.debug_f "on_consensus=> local_store n=%s i=%s"
    (Sn.string_of n) (Sn.string_of i)
  >>= fun () ->
  let m_store_i = store # consensus_i () in
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
  if store # quiesced () 
  then
    begin
      store # with_transaction (fun tx -> store # incr_i tx) >>= fun () ->
      Lwt.return [Ok None]
    end
  else
    store # with_transaction_lock (fun key -> _insert_value store v (Key key))
      
let get_succ_store_i (store:store) =
  let m_si = store # consensus_i () in
  match m_si with
    | None -> Sn.start
    | Some si -> Sn.succ si

let get_catchup_start_i = get_succ_store_i
