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

let _filter get_key make_entry fold coll pf =
  let pl = String.length pf in
  fold (fun acc entry ->
    let key = get_key entry in
    let kl = String.length key in
    let key' = String.sub key pl (kl-pl) in
    (make_entry entry key')::acc) [] coll

let filter_keys_list keys =
  _filter (fun i -> i) (fun e k -> k) List.fold_left keys __prefix

let filter_entries_list entries =
  _filter (fun (k,v) -> k) (fun (k,v) k' -> (k',v)) List.fold_left entries __prefix

let filter_keys_array entries =
  _filter (fun i -> i) (fun e k -> k) Array.fold_left entries __prefix


class transaction = object end
class transaction_lock = object end

(** common interface for stores *)
class type simple_store = object
  method with_transaction_lock: (transaction_lock -> 'a Lwt.t) -> 'a Lwt.t
  method with_transaction: ?key: transaction_lock option -> (transaction -> 'a Lwt.t) -> 'a Lwt.t

  method exists: string -> bool
  method get: string -> string

  method range: string -> string option -> bool -> string option -> bool -> int -> string list
  method range_entries: string -> string option -> bool -> string option -> bool -> int -> (string * string) list
  method rev_range_entries: string -> string option -> bool -> string option -> bool -> int -> (string * string) list
  method prefix_keys: string -> int -> string list
  method set: transaction -> string -> string -> unit
  method delete: transaction -> string -> unit
  method delete_prefix: transaction -> string -> int

  method close: unit -> unit Lwt.t
  method reopen: (unit -> unit Lwt.t) -> bool -> unit Lwt.t

  method get_location: unit -> string
  method relocate: string -> unit Lwt.t

  method get_key_count : unit -> int64 Lwt.t

  method optimize : bool -> unit Lwt.t
  method defrag : unit -> unit Lwt.t
  method copy_store : ?_networkClient: bool -> Lwt_io.output_channel -> unit Lwt.t
  method get_fringe : string option -> Routing.range_direction -> (string * string) list Lwt.t

end


type store = { s : simple_store;
               (** last value on which there is consensus.
                   For an empty store, This is None
               *)
               mutable store_i : Sn.t option;
               mutable master : (string * int64) option;
               mutable interval : Interval.t;
               mutable routing : Routing.t option;
               mutable quiesced : bool;
               mutable closed : bool;
             }

let _get_interval (store:simple_store) =
  try
    let interval_s = store # get __interval_key in
    let interval,_ = Interval.interval_from interval_s 0 in
    interval
  with Not_found -> Interval.max

let _consensus_i (store:simple_store) =
  try
    let i_string = store # get __i_key in
    let i,_ = Sn.sn_from i_string 0 in
    Some i
  with Not_found ->
    None

let _master (store:simple_store) =
  try
    let m = store # get __master_key in
    let ls_buff = store # get __lease_key in
    let ls,_ = Llio.int64_from ls_buff 0 in
    Some (m,ls)
  with Not_found ->
    None

let _get_routing (store:simple_store) =
  try
    let routing_s = store # get __routing_key in
    let routing,_ = Routing.routing_from routing_s 0 in
    Some routing
  with Not_found -> None


let make_store simple_store =
  let store_i = _consensus_i simple_store
  and master = _master simple_store
  and interval = _get_interval simple_store
  and routing = _get_routing simple_store in
  { s = simple_store;
    store_i;
    master;
    interval;
    routing;
    quiesced = false;
    closed = false;
  }

type update_result =
  | Ok of string option
  | Update_fail of Arakoon_exc.rc * string

exception Key_not_found of string
exception CorruptStore

let _get (store:store) key =
  store.s # get (__prefix ^ key)

let get (store:store) key =
  try
    Lwt.return (_get store key)
  with
    | Failure msg ->
        let _closed = store.closed in
        Lwt_log.debug_f "local_store: Failure %Swhile GET (_closed:%b)" msg _closed >>= fun () ->
        if _closed
        then
          Lwt.fail (Common.XException (Arakoon_exc.E_GOING_DOWN,
                                       Printf.sprintf "GET %S database already closed" key))
        else
          Lwt.fail CorruptStore
	| exn -> Lwt.fail exn

let _set (store:store) tx key value =
  store.s # set tx (__prefix ^ key) value

let get_option (store:store) key =
  try
    Some (store.s # get (__prefix ^ key))
  with Not_found -> None

let exists (store:store) key =
  Lwt.return (store.s # exists (__prefix ^ key))

let multi_get (store:store) keys =
  let vs = List.fold_left (fun acc key ->
    try
      let v = store.s # get (__prefix ^ key) in
      v::acc
    with Not_found ->
      let exn = Common.XException(Arakoon_exc.E_NOT_FOUND, key) in
      raise exn)
    [] keys
  in
  Lwt.return (List.rev vs)

let consensus_i (store:store) =
  store.store_i

let get_location (store:store) =
  store.s # get_location ()

let reopen (store:store) f =
  store.s # reopen f store.quiesced >>= fun () ->
  store.store_i <- _consensus_i store.s;
  store.closed <- false;
  Lwt.return ()

let close (store:store) =
  store.closed <- true;
  store.s # close ()

let relocate (store:store) loc =
  store.s # relocate loc

let quiesced (store:store) =
  store.quiesced

let quiesce (store:store) =
  if store.quiesced
  then Lwt.fail(Failure "Store already quiesced. Blocking second attempt")
  else
    begin
      store.quiesced <- true;
	  reopen store (fun () -> Lwt.return ()) >>= fun () ->
	  Lwt.return ()
    end

let unquiesce (store:store) =
  store.quiesced <- false;
  reopen store (fun () -> Lwt.return ())

let optimize (store:store) =
  store.s # optimize store.quiesced

let set_master (store:store) tx master lease_start =
  store.s # set tx __master_key master;
  let buffer = Buffer.create 8 in
  let () = Llio.int64_to buffer lease_start in
  let lease = Buffer.contents buffer in
  store.s # set tx __lease_key lease;
  store.master <- Some (master, lease_start);
  Lwt.return ()

let set_master_no_inc (store:store) master lease_start =
  if store.quiesced
  then
    begin
      store.master <- Some (master, lease_start);
      Lwt.return ()
    end
  else
    store.s # with_transaction (fun tx -> set_master store tx master lease_start)

let who_master (store:store) =
  store.master

let consensus_i (store:store) =
  store.store_i

let incr_i (store:store) tx =
  let old_i = _consensus_i store.s in
  let new_i =
    match old_i with
      | None -> Sn.start
      | Some i -> Sn.succ i
  in
  let new_is =
    let buf = Buffer.create 10 in
    let () = Sn.sn_to buf new_i in
    Buffer.contents buf
  in
  let () = store.s # set tx __i_key new_is in
  store.store_i <- Some new_i;
  Lwt_log.debug_f "Store.incr_i old_i:%s -> new_i:%s"
     (Log_extra.option2s Sn.string_of old_i) (Sn.string_of new_i)

let with_transaction_lock (store:store) f =
  store.s # with_transaction_lock f

let get_fringe (store:store) b d =
  store.s # get_fringe b d

let copy_store (store:store) oc =
  if store.quiesced
  then
    store.s # copy_store oc
  else
    let ex = Common.XException(Arakoon_exc.E_UNKNOWN_FAILURE, "Can only copy a quiesced store" ) in
    raise ex

let defrag (store:store) =
  store.s # defrag ()

let get_key_count (store:store) =
  store.s # get_key_count () >>= fun raw_count ->
  (* Leave out administrative keys *)
  let admin_keys = store.s # prefix_keys __adminprefix (-1) in
  let admin_key_count = List.length admin_keys in
  Lwt.return ( Int64.sub raw_count (Int64.of_int admin_key_count) )

let get_routing (store:store) =
  Lwt_log.debug "get_routing " >>= fun () ->
  match store.routing with
    | None -> Lwt.fail Not_found
    | Some r -> Lwt.return r

let _set_routing store tx routing =
  let buf = Buffer.create 80 in
  let () = Routing.routing_to buf routing in
  let routing_s = Buffer.contents buf in
  store.s # set tx __routing_key routing_s;
  store.routing <- Some routing

let _set_routing_delta store tx left sep right =
  let new_r =
    begin
      match store.routing with
        | None ->
          begin
            Routing.build ([(left, sep)], right)
          end
        | Some r ->
          begin
            Routing.change r left sep right
          end
      end
  in
  let new_r' = Routing.compact new_r in
  _set_routing store tx new_r'


let with_transaction (store:store) ?(key=None) f =
  if store.closed
  then
    raise (Common.XException (Arakoon_exc.E_GOING_DOWN, "opening a transaction while database is closed"))
  else
    let current_i = store.store_i in
    Lwt.catch
      (fun () -> store.s # with_transaction ~key f)
      (fun exn ->
        store.store_i <- current_i;
        match exn with
          | Failure s -> Lwt_log.debug_f "Failure %s" s >>= fun () ->
              Lwt.fail Server.FOOBAR
          | exn -> Lwt.fail exn)

let multi_get_option (store:store) keys =
  let vs = List.fold_left (fun acc key ->
    try
      let v = store.s # get (__prefix ^ key) in
      (Some v)::acc
    with Not_found -> None::acc)
    [] keys
  in
  Lwt.return (List.rev vs)

let _delete (store:store) tx key =
  store.s # delete tx (__prefix ^ key)

let _get_j (store:store) =
  try
    let jstring = store.s # get __j_key in
    int_of_string jstring
  with Not_found -> 0

let _set_j (store:store) tx j =
  store.s # set tx __j_key (string_of_int j)

let _test_and_set (store:store) tx key expected wanted =
  let existing = get_option store key in
  if existing <> expected
  then
    existing
  else
    begin
      let () = match wanted with
        | None -> store.s # delete tx (__prefix ^ key)
        | Some wanted_s -> store.s # set tx (__prefix ^ key) wanted_s in
      wanted
    end

let _range_entries (store:store) first finc last linc max =
  store.s # range_entries __prefix first finc last linc max

let range_entries (store:store) ?(_pf=__prefix) first finc last linc max =
  Lwt.return (store.s # range_entries _pf first finc last linc max)

let rev_range_entries (store:store) first finc last linc max =
  Lwt.return (store.s # rev_range_entries __prefix first finc last linc max)

let range (store:store) first finc last linc max =
  Lwt_log.debug_f "%s %s %s" __prefix (string_option2s first) (string_option2s last) >>= fun () ->
  Lwt.return (store.s # range __prefix None finc last linc max)

let prefix_keys (store:store) prefix max =
  Lwt.return (store.s # prefix_keys (__prefix ^ prefix) max)

let get_interval store =
  Lwt.return (store.interval)

let _set_interval store tx range =
  let buf = Buffer.create 80 in
  let () = Interval.interval_to buf range in
  let range_s = Buffer.contents buf in
  store.s # set tx __interval_key range_s

class store_user_db store tx =
  let test k =
    if not (Interval.is_ok store.interval k) then
      raise (Common.XException (Arakoon_exc.E_OUTSIDE_INTERVAL, k))
  in
  let test_option = function
    | None -> ()
    | Some k -> test k
  in
  let test_range first last = test_option first; test_option last
  in

object (self : # Registry.user_db)

  method set k v = test k ; _set store tx k v
  method get k   = test k ; _get store k

  method delete k = test k ; _delete store tx k

  method test_and_set k e w = test k; _test_and_set store tx k e w

  method range_entries first finc last linc max =
    test_range first last;
    _range_entries store first finc last linc max
end

let _user_function (store:store) (name:string) (po:string option) tx =
  Lwt.wrap (fun () ->
    let f = Registry.Registry.lookup name in
    let user_db = new store_user_db store tx in
    let ro = f user_db po in
    ro)


type key_or_transaction =
  | Key of transaction_lock
  | Transaction of transaction

let _with_transaction : store -> key_or_transaction -> (transaction -> 'a Lwt.t) -> 'a Lwt.t =
  fun store kt f -> match kt with
    | Key key ->
        store.s # with_transaction ~key:(Some key) f
    | Transaction tx ->
        f tx

let _insert_update (store:store) (update:Update.t) kt =
  let rec _do_one update tx =
    let return () = Lwt.return (Ok None) in
    let wrap f = (Lwt.wrap f) >>= return in
    match update with
      | Update.Set(key,value) -> wrap (fun () -> _set store tx key value)
      | Update.MasterSet (m, lease) -> set_master store tx m lease >>= return
      | Update.Delete(key) ->
          Lwt_log.debug_f "store # delete %S" key >>= fun () ->
          wrap (fun () -> _delete store tx key)
      | Update.DeletePrefix prefix ->
          Lwt_log.debug_f "store :: delete_prefix %S" prefix >>= fun () ->
          let n_deleted = store.s # delete_prefix tx (__prefix ^ prefix) in
          let sb = Buffer.create 8 in
          let () = Llio.int_to sb n_deleted in
          let ser = Buffer.contents sb in
          Lwt.return (Ok (Some ser))
      | Update.TestAndSet(key,expected,wanted)->
                let existing = get_option store key in
                if existing = expected
                then
                  begin
                    match wanted with
                      | None -> store.s # delete tx (__prefix ^ key)
                      | Some wanted_s -> store.s # set tx (__prefix ^ key) wanted_s
                  end;
                Lwt.return (Ok existing)
      | Update.UserFunction(name,po) ->
              _user_function store name po tx >>= fun ro ->
              Lwt.return (Ok ro)
      | Update.Sequence updates
      | Update.SyncedSequence updates ->
              Lwt_list.iter_s (fun update ->
                _do_one update tx >>= function
                  | Update_fail (rc, msg) -> Lwt.fail (Arakoon_exc.Exception(rc, msg))
                  | _ -> Lwt.return ()) updates >>= fun () -> Lwt.return (Ok None)
      | Update.SetInterval interval ->
              wrap (fun () -> _set_interval store tx interval)
      | Update.SetRouting routing ->
          Lwt_log.debug_f "set_routing %s" (Routing.to_s routing) >>= fun () ->
          wrap (fun () -> _set_routing store tx routing)
      | Update.SetRoutingDelta (left, sep, right) ->
          Lwt_log.debug "local_store::set_routing_delta" >>= fun () ->
          wrap (fun () -> _set_routing_delta store tx left sep right)
      | Update.Nop -> Lwt.return (Ok None)
      | Update.Assert(k,vo) ->
          begin
            match vo, get_option store k with
              | None, None -> Lwt.return (Ok None)
              | Some v, Some v' when v = v' -> Lwt.return (Ok None)
              | _ -> Lwt.return (Update_fail(Arakoon_exc.E_ASSERTION_FAILED,k))
          end
      | Update.Assert_exists(k) ->
          begin
            match store.s # exists (__prefix ^ k) with
	          | true -> Lwt.return (Ok None)
	          | false -> Lwt.return (Update_fail(Arakoon_exc.E_ASSERTION_FAILED,k))
          end
      | Update.AdminSet(k,vo) ->
              let () =
                match vo with
                  | None   -> store.s # delete tx (__adminprefix ^ k)
                  | Some v -> store.s # set    tx (__adminprefix ^ k) v
              in
              Lwt.return (Ok None)
  in
  let with_transaction' f = _with_transaction store kt f in
  let catch_with_tx f g =
    Lwt.catch
      (fun () ->
        let j = _get_j store in
        Lwt.finalize
          (fun () -> with_transaction' (fun tx -> f tx))
          (fun () -> with_transaction' (fun tx -> Lwt.return (_set_j store tx (j + 1)))))
      g in
  let with_error notfound_msg f =
    catch_with_tx
      (fun tx -> f tx)
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
  let do_one update =
    match update with
      | Update.Set(key,value) ->
          with_error key (fun tx -> _do_one update tx)
      | Update.MasterSet (m, lease) ->
          with_error "Not_found" (fun tx -> _do_one update tx)
      | Update.Delete(key) ->
          with_error key (fun tx -> _do_one update tx)
      | Update.DeletePrefix prefix ->
          begin
            catch_with_tx
              (fun tx -> _do_one update tx)
              (fun e -> 
                let rc = Arakoon_exc.E_UNKNOWN_FAILURE
                and msg = Printexc.to_string e
                in
                Lwt.return (Update_fail (rc,msg)))
          end
      | Update.TestAndSet(key,expected,wanted)->
          begin
            catch_with_tx
              (fun tx -> _do_one update tx)
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
          catch_with_tx
            (fun tx -> _do_one update tx)
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
          catch_with_tx
            (fun tx -> _do_one update tx)
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
              | Common.XException (rc, msg) -> Lwt.return (Update_fail(rc,msg))
              | e ->
                  let rc = Arakoon_exc.E_UNKNOWN_FAILURE
                  and msg = Printexc.to_string e
                  in
                  Lwt.return (Update_fail (rc,msg))
            )
      | Update.SetInterval interval ->
          catch_with_tx
            (fun tx -> _do_one update tx)
            (function
              | Common.XException (rc,msg) -> Lwt.return (Update_fail(rc,msg))
              | e ->
                  let rc = Arakoon_exc.E_UNKNOWN_FAILURE
                  and msg = Printexc.to_string e
                  in
                  Lwt.return (Update_fail (rc,msg)))
      | Update.SetRouting routing ->
          catch_with_tx
            (fun tx -> _do_one update tx)
            (function
              | Common.XException (rc, msg) -> Lwt.return (Update_fail(rc,msg))
              | e ->
                  let rc = Arakoon_exc.E_UNKNOWN_FAILURE
                  and msg = Printexc.to_string e
                  in
                  Lwt.return (Update_fail (rc,msg))
            )
      | Update.SetRoutingDelta (left, sep, right) ->
          catch_with_tx
            (fun tx -> _do_one update tx)
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
          catch_with_tx
            (fun tx -> _do_one update tx)
	        (fun e ->
	          let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	          and msg = Printexc.to_string e
	          in Lwt.return (Update_fail(rc, msg)))
      | Update.Assert_exists(k) ->
          catch_with_tx
            (fun tx -> _do_one update tx)
	        (fun e ->
	          let rc = Arakoon_exc.E_UNKNOWN_FAILURE
	          and msg = Printexc.to_string e
	          in Lwt.return (Update_fail(rc, msg)))
      | Update.AdminSet(k,vo) ->
          catch_with_tx
            (fun tx -> _do_one update tx)
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
    do_one update
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
  let j = _get_j store in
  let skip n l =
    let rec inner = function
      | 0, l -> l
      | n, [] -> failwith "need to skip more updates than present in this paxos value"
      | n, hd::tl -> inner ((n - 1), tl) in
    inner (n, l) in
  let updates' = skip j updates in
  Lwt_log.debug_f "skipped %i updates" j >>= fun () ->
  _insert_updates store updates' kt >>= fun (urs:update_result list) ->
  _with_transaction store kt (fun tx ->
    incr_i store tx >>= fun () ->
    Lwt.return (_set_j store tx 0)) >>= fun () ->
  Lwt.return urs


let safe_insert_value (store:store) ?(tx=None) (i:Sn.t) value =
  let inner f =
    match tx with
      | None ->   store.s # with_transaction_lock (fun key -> f (Key key))
      | Some tx -> f (Transaction tx) in
  let t kt =
    let store_i = consensus_i store in
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
    if store.quiesced
    then
      begin
        _with_transaction store kt (fun tx -> incr_i store tx) >>= fun () ->
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
  let m_store_i = consensus_i store in
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
  if store.quiesced
  then
    begin
      store.s # with_transaction (fun tx -> incr_i store tx) >>= fun () ->
      Lwt.return [Ok None]
    end
  else
    store.s # with_transaction_lock (fun key -> _insert_value store v (Key key))
      
let get_succ_store_i (store:store) =
  let m_si = consensus_i store in
  match m_si with
    | None -> Sn.start
    | Some si -> Sn.succ si

let get_catchup_start_i = get_succ_store_i
