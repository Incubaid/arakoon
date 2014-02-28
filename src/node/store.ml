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

open Std
open Update
open Interval
open Routing
open Lwt
open Log_extra
open Simple_store


type update_result =
    Simple_store.update_result =
  | Ok of string option
  | Update_fail of Arakoon_exc.rc * string

exception CorruptStore

type key_or_transaction =
  | Key of transaction_lock
  | Transaction of transaction

module type STORE =
sig
  type ss
  type t
  val make_store : lcnum:int -> ncnum:int -> ?read_only:bool -> string -> t Lwt.t
  val consensus_i : t -> Sn.t option
  val flush : t -> unit Lwt.t
  val close : ?flush : bool -> t -> unit Lwt.t
  val get_location : t -> string
  val reopen : t -> (unit -> unit Lwt.t) -> unit Lwt.t
  val safe_insert_value : t -> Sn.t -> Value.t -> update_result list Lwt.t
  val with_transaction : t -> (transaction -> 'a Lwt.t) -> 'a Lwt.t
  val relocate : t -> string -> unit Lwt.t

  val quiesced : t -> bool
  val quiesce : Quiesce.Mode.t -> t -> unit Lwt.t
  val unquiesce : t -> unit Lwt.t
  val optimize : t -> unit Lwt.t
  val defrag : t -> unit Lwt.t
  val copy_store : t -> Lwt_io.output_channel -> unit Lwt.t
  val copy_store2 : string -> string -> bool -> unit Lwt.t

  val get_succ_store_i : t -> int64
  val get_catchup_start_i : t -> int64

  val incr_i : t -> unit Lwt.t

  val set_master : t -> transaction -> string -> float -> unit Lwt.t
  val set_master_no_inc : t -> string -> float -> unit Lwt.t
  val clear_self_master : t -> string -> unit
  val who_master : t -> (string * float) option

  val get : t -> string -> string Lwt.t
  val exists : t -> string -> bool Lwt.t
  val range :  t ->
    string option -> bool ->
    string option -> bool -> int -> string array Lwt.t
  val range_entries :  t -> ?_pf:string ->
    string option -> bool ->
    string option -> bool -> int -> (string * string) array Lwt.t
  val rev_range_entries :  t ->
    string option -> bool ->
    string option -> bool -> int -> (string * string) array Lwt.t
  val prefix_keys : t -> string -> int -> string list Lwt.t
  val multi_get : t -> string list -> string list Lwt.t
  val multi_get_option : t -> string list -> string option list Lwt.t
  val get_key_count : t -> int64 Lwt.t

  val get_fringe :  t -> string option -> Routing.range_direction -> (string * string) list Lwt.t

  val get_interval : t -> Interval.t Lwt.t
  val get_routing : t -> Routing.t Lwt.t

  val on_consensus : t -> Value.t * int64 * Int64.t -> update_result list Lwt.t

  val _set_i : t -> Sn.t -> unit
end

module Make(S : Simple_store) =
struct
  type ss = S.t
  type t = { s : ss;
             (** last value on which there is consensus.
                 For an empty store, This is None
             *)
             mutable store_i : Sn.t option;
             mutable master : (string * float) option;
             mutable interval : Interval.t;
             mutable routing : Routing.t option;
             mutable quiesced : Quiesce.Mode.t;
             mutable closed : bool;
             mutable _tx_lock : transaction_lock option;
             _tx_lock_mutex : Lwt_mutex.t
           }

  let _get_interval store =
    try
      let interval_s = S.get store __interval_key in
      let interval,_ = Interval.interval_from interval_s 0 in
      interval
    with Not_found -> Interval.max

  let _consensus_i store =
    try
      let i_string = S.get store __i_key in
      let i,_ = Sn.sn_from i_string 0 in
      Some i
    with Not_found ->
      None

  let _master store =
    try
      let m = S.get store __master_key in
      let ls =
        try
          (* first try new key with more accurate storage *)
          let ls_buff = S.get store __lease_key2 in
          let ls, _ = Llio.float_from ls_buff 0 in
          ls
        with Not_found ->
          (* fallback to old more coarse grained lease period *)
          let ls_buff = S.get store __lease_key in
          let ls, _ = Llio.int64_from ls_buff 0 in
          (Int64.to_float ls) +. 1. in
      Some (m,ls)
    with Not_found ->
      None

  let _get_routing store =
    try
      let routing_s = S.get store __routing_key in
      let routing,_ = Routing.routing_from routing_s 0 in
      Some routing
    with Not_found -> None

  let initialize store =
    store.store_i <- _consensus_i store.s;
    store.master <- _master store.s;
    store.interval <- _get_interval store.s;
    store.routing <- _get_routing store.s

  let make_store
      ~lcnum
      ~ncnum
      ?(read_only=false) db_name =
    S.make_store ~lcnum ~ncnum read_only db_name >>= fun simple_store ->
    let store =
      { s = simple_store;
        store_i = None;
        master = None;
        interval = Interval.max;
        routing = None;
        quiesced = Quiesce.Mode.NotQuiesced;
        closed = false;
        _tx_lock = None;
        _tx_lock_mutex = Lwt_mutex.create ()
      } in
    let () = initialize store in
    Lwt.return store

  let _get store key =
    S.get store.s (__prefix ^ key)

  let _wrap_exception store name not_closed_failure_exn f =
    Lwt.catch
      f
      (function
        | Failure s ->
          begin
            Logger.debug_f_ "store: %s Failure %s: => FOOBAR? (_closed:%b)" name s store.closed >>= fun () ->
            if store.closed
            then Lwt.fail (Arakoon_exc.Exception (Arakoon_exc.E_GOING_DOWN, s ^ " : database closed"))
            else Lwt.fail not_closed_failure_exn
          end
        | exn -> Lwt.fail exn)

  let get store key =
    _wrap_exception store "GET" CorruptStore (fun () ->
        Lwt.return (_get store key))

  let _set store tx key value =
    S.set store.s tx (__prefix ^ key) value

  let _get_option store key =
    try
      Some (_get store key)
    with Not_found -> None

  let exists store key =
    _wrap_exception store "EXISTS" CorruptStore (fun () ->
        Lwt.return (S.exists store.s (__prefix ^ key)))

  let multi_get store keys =
    _wrap_exception store "MULTI_GET" CorruptStore (fun () ->
        let vs = List.fold_left (fun acc key ->
            try
              let v = _get store key in
              v::acc
            with Not_found ->
              let exn = Common.XException(Arakoon_exc.E_NOT_FOUND, key) in
              raise exn)
            [] keys
        in
        Lwt.return (List.rev vs))

  let consensus_i store =
    store.store_i

  let get_location store =
    S.get_location store.s

  let reopen store f =
    let m = match store.quiesced with
      | Quiesce.Mode.NotQuiesced | Quiesce.Mode.Writable -> false
      | Quiesce.Mode.ReadOnly -> true in
    S.reopen store.s f m >>= fun () ->
    initialize store;
    store.closed <- false;
    Lwt.return ()

  let flush store =
    S.flush store.s

  let close ?(flush = true) store =
    store.closed <- true;
    Logger.debug_ "closing store..." >>= fun () ->
    S.close store.s flush >>= fun () ->
    Logger.debug_ "closed store"

  let relocate store loc =
    S.relocate store.s loc

  let quiesced store = Quiesce.Mode.is_quiesced store.quiesced

  let quiesce mode store =
    if quiesced store
    then Lwt.fail(Failure "Store already quiesced. Blocking second attempt")
    else
      begin
        store.quiesced <- mode;
        reopen store (fun () -> Lwt.return ()) >>= fun () ->
        Lwt.return ()
      end

  let unquiesce store =
    store.quiesced <- Quiesce.Mode.NotQuiesced;
    reopen store (fun () -> Lwt.return ())

  let optimize store =
    let m = match store.quiesced with
      | Quiesce.Mode.NotQuiesced | Quiesce.Mode.Writable -> false
      | Quiesce.Mode.ReadOnly -> true in
    S.optimize store.s m

  let set_master store tx master lease_start =
    _wrap_exception store "SET_MASTER" Server.FOOBAR (fun () ->
        S.set store.s tx __master_key master;
        let buffer = Buffer.create 8 in
        let () = Llio.float_to buffer lease_start in
        let lease = Buffer.contents buffer in
        S.set store.s tx __lease_key2 lease;
        store.master <- Some (master, lease_start);
        Lwt.return ())

  let set_master_no_inc store master lease_start =
    if quiesced store
    then
      begin
        store.master <- Some (master, lease_start);
        Lwt.return ()
      end
    else
      S.with_transaction store.s (fun tx -> set_master store tx master lease_start)

  let clear_self_master store me =
    match store.master with
      | Some (m, ls) when m = me -> store.master <- None
      | _ -> ()

  let who_master store =
    store.master

  let consensus_i store =
    store.store_i

  let _get_j store =
    try
      let jstring = S.get store.s __j_key in
      int_of_string jstring
    with Not_found -> 0

  let _set_j store tx j =
    S.set store.s tx __j_key (string_of_int j)

  let _new_i = function
    | None -> Sn.start
    | Some i -> Sn.succ i

  let _incr_i store tx =
    let old_i = _consensus_i store.s in
    let new_i = _new_i old_i in
    let new_is =
      let buf = Buffer.create 10 in
      let () = Sn.sn_to buf new_i in
      Buffer.contents buf
    in
    let () = S.set store.s tx __i_key new_is in
    let () = _set_j store tx 0 in
    store.store_i <- Some new_i;
    Logger.debug_f_ "Store.incr_i old_i:%s -> new_i:%s"
      (Log_extra.option2s Sn.string_of old_i) (Sn.string_of new_i)

  let incr_i store =
    if quiesced store
    then
      begin
        let new_i = _new_i store.store_i in
        store.store_i <- Some new_i;
        Lwt.return ()
      end
    else
      S.with_transaction store.s (fun tx -> _incr_i store tx)

  let _set_i store i =
    if quiesced store
    then
      store.store_i <- Some i
    else
      failwith "_set_i is only meant to be used on a quiesced store to cheat with tlog replay"

  let _with_transaction_lock store f =
    Lwt_mutex.with_lock store._tx_lock_mutex (fun () ->
        Lwt.finalize
          (fun () ->
             let txl = new transaction_lock in
             store._tx_lock <- Some txl;
             f txl)
          (fun () -> store._tx_lock <- None; Lwt.return ()))

  let get_fringe store b d =
    S.get_fringe store.s b d

  let copy_store store oc =
    if quiesced store
    then
      S.copy_store store.s true oc
    else
      let ex = Common.XException(Arakoon_exc.E_UNKNOWN_FAILURE, "Can only copy a quiesced store" ) in
      raise ex

  let copy_store2 old_location new_location overwrite =
    (* TODO quiesced checking *)
    S.copy_store2 old_location new_location overwrite

  let defrag store =
    S.defrag store.s

  let get_key_count store =
    _wrap_exception store "GET_KEY_COUNT" CorruptStore (fun () ->
        S.get_key_count store.s >>= fun raw_count ->
        (* Leave out administrative keys *)
        let admin_keys = S.prefix_keys store.s __adminprefix (-1) in
        let admin_key_count = List.length admin_keys in
        Lwt.return ( Int64.sub raw_count (Int64.of_int admin_key_count) ))

  let get_routing store =
    Logger.debug_ "get_routing " >>= fun () ->
    match store.routing with
      | None -> Lwt.fail Not_found
      | Some r -> Lwt.return r

  let _set_routing store tx routing =
    let buf = Buffer.create 80 in
    let () = Routing.routing_to buf routing in
    let routing_s = Buffer.contents buf in
    S.set store.s tx __routing_key routing_s;
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


  let with_transaction store f =
    if store.closed
    then
      raise (Common.XException (Arakoon_exc.E_GOING_DOWN, "opening a transaction while database is closed"))
    else
      let current_i = store.store_i in
      Lwt.catch
        (fun () -> S.with_transaction store.s f)
        (fun exn ->
           store.store_i <- current_i;
           match exn with
             | Failure s -> Logger.debug_f_ "Failure %s" s >>= fun () ->
               Lwt.fail Server.FOOBAR
             | exn -> Lwt.fail exn)

  let multi_get_option store keys =
    _wrap_exception store "MULTI_GET_OPTION" CorruptStore (fun () ->
        let vs = List.fold_left (fun acc key ->
            try
              let v = _get store key in
              (Some v)::acc
            with Not_found -> None::acc)
            [] keys
        in
        Lwt.return (List.rev vs))

  let _delete store tx key =
    S.delete store.s tx (__prefix ^ key)

  let _test_and_set store tx key expected wanted =
    let existing = _get_option store key in
    if existing = expected
    then
      begin
        match wanted with
          | None ->
            begin
              match existing with
                | None -> ()
                | Some _ -> _delete store tx key
            end
          | Some wanted_s -> _set store tx key wanted_s
      end;
    existing

  let _replace store tx key wanted =
    let r = _get_option store key in
    begin
      match wanted,r with
      | Some v, _      -> _set store tx key v
      | None  , None   -> ()
      | None  , Some o -> _delete store tx key
    end;
    r

  let _range_entries store first finc last linc max =
    S.range_entries store.s __prefix first finc last linc max

  let range_entries store ?(_pf=__prefix) first finc last linc max =
    _wrap_exception store "RANGE_ENTRIES" CorruptStore (fun () ->
        Lwt.return (S.range_entries store.s _pf first finc last linc max))

  let rev_range_entries store first finc last linc max =
    _wrap_exception store "REV_RANGE_ENTRIES" CorruptStore (fun () ->
        Lwt.return (S.rev_range_entries store.s __prefix first finc last linc max))

  let range store first finc last linc max =
    _wrap_exception
      store
      "RANGE"
      CorruptStore
      (fun () ->
       let first = match first with
         | None -> __prefix
         | Some f -> __prefix ^ f in
       let last = match last with
         | None -> next_prefix __prefix
         | Some l -> Some (__prefix ^ l) in
       let comp_last =
         match last with
         | None ->
            fun k -> true
         | Some last ->
            if linc
            then
              fun k -> String.(<=:) k last
            else
              fun k -> String.(<:) k last in
       let p = String.length __prefix in
       let cut x = String.sub x p (String.length x - p) in
       let res =
         S.with_cursor
           store.s
           (fun cur ->
            let rec inner acc count =
              if count = max
              then
                acc
              else
                begin
                  let k, _ = S.cur_get cur in
                  if comp_last k
                  then
                    begin
                      let acc' = (cut k) :: acc in
                      if S.cur_next cur
                      then
                        inner acc' (count + 1)
                      else
                        acc'
                    end
                  else
                    acc
                end
            in
            if S.cur_jump cur first
            then
              inner [] 0
            else
              [])
       in
       let r = Array.of_list res in
       Lwt.return r)

  let prefix_keys store prefix max =
    _wrap_exception store "PREFIX_KEYS" CorruptStore (fun () ->
        Lwt.return (S.prefix_keys store.s (__prefix ^ prefix) max))

  let get_interval store =
    Lwt.return (store.interval)

  let _set_interval store tx range =
    let buf = Buffer.create 80 in
    let () = Interval.interval_to buf range in
    let range_s = Buffer.contents buf in
    S.set store.s tx __interval_key range_s

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

  let _user_function store (name:string) (po:string option) tx =
    Lwt.wrap (fun () ->
        let f = Registry.Registry.lookup name in
        let user_db = new store_user_db store tx in
        let ro = f user_db po in
        ro)


  let _with_transaction : t -> key_or_transaction -> (transaction -> 'a Lwt.t) -> 'a Lwt.t =
    fun store kt f -> match kt with
      | Key key ->
        let matched_locks = match store._tx_lock with
          | Some txl -> txl == key (* txl should be the same instance (physical equality) *)
          | _ -> false in
        if not matched_locks
        then failwith "transaction locks do not match";
        with_transaction store f
      | Transaction tx ->
        f tx

  let _insert_update store (update:Update.t) kt =
    let rec _do_one update tx =
      let return () = Lwt.return (Ok None) in
      let wrap f =
        _wrap_exception store "_insert_update" Server.FOOBAR (fun () ->
            (Lwt.wrap f) >>= return) in
      match update with
        | Update.Set(key,value) -> wrap (fun () -> _set store tx key value)
        | Update.MasterSet (m, lease) -> set_master store tx m lease >>= return
        | Update.Delete(key) ->
          Logger.debug_f_ "store # delete %S" key >>= fun () ->
          wrap (fun () -> _delete store tx key)
        | Update.DeletePrefix prefix ->
          Logger.debug_f_ "store :: delete_prefix %S" prefix >>= fun () ->
          let n_deleted = S.delete_prefix store.s tx (__prefix ^ prefix) in
          let sb = Buffer.create 8 in
          let () = Llio.int_to sb n_deleted in
          let ser = Buffer.contents sb in
          Lwt.return (Ok (Some ser))
        | Update.TestAndSet(key,expected,wanted)->
          Lwt.return (Ok (_test_and_set store tx key expected wanted))
        | Update.Replace (key,wanted) ->
           Lwt.return (Ok (_replace store tx key wanted))
        | Update.UserFunction(name,po) ->
          _user_function store name po tx >>= fun ro ->
          Lwt.return (Ok ro)
        | Update.Sequence updates
        | Update.SyncedSequence updates ->
          let get_key = function
            | Update.Set (key,value) -> Some key
            | Update.Delete key -> Some key
            | Update.TestAndSet (key, expected, wanted) -> Some key
            | _ -> None in
          Lwt_list.iter_s (fun update ->
              (Lwt.catch
                 (fun () -> _do_one update tx)
                 (function
                   | Not_found ->
                     begin
                       match get_key update with
                         | Some key -> Lwt.fail (Key_not_found key)
                         | None -> Lwt.fail Not_found
                     end
                   | exn -> Lwt.fail exn))
              >>= function
              | Update_fail (rc, msg) -> Lwt.fail (Arakoon_exc.Exception(rc, msg))
              | _ -> Lwt.return ()) updates >>= fun () -> Lwt.return (Ok None)
        | Update.SetInterval interval ->
          wrap (fun () -> _set_interval store tx interval)
        | Update.SetRouting routing ->
          Logger.debug_f_ "set_routing %s" (Routing.to_s routing) >>= fun () ->
          wrap (fun () -> _set_routing store tx routing)
        | Update.SetRoutingDelta (left, sep, right) ->
          Logger.debug_ "local_store::set_routing_delta" >>= fun () ->
          wrap (fun () -> _set_routing_delta store tx left sep right)
        | Update.Nop -> Lwt.return (Ok None)
        | Update.Assert(k,vo) ->
          begin
            match vo, _get_option store k with
              | None, None -> Lwt.return (Ok None)
              | Some v, Some v' when v = v' -> Lwt.return (Ok None)
              | _ -> Lwt.return (Update_fail(Arakoon_exc.E_ASSERTION_FAILED,k))
          end
        | Update.Assert_exists(k) ->
          begin
            match S.exists store.s (__prefix ^ k) with
              | true -> Lwt.return (Ok None)
              | false -> Lwt.return (Update_fail(Arakoon_exc.E_ASSERTION_FAILED,k))
          end
        | Update.AdminSet(k,vo) ->
          let () =
            match vo with
              | None   -> S.delete store.s tx (__adminprefix ^ k)
              | Some v -> S.set    store.s tx (__adminprefix ^ k) v
          in
          Lwt.return (Ok None)
    in
    let with_transaction' f = _with_transaction store kt f in
    let update_in_tx f =
      let j = _get_j store in
      Lwt.catch
        (fun () ->
           with_transaction'
             (fun tx ->
                f tx >>= fun a ->
                _set_j store tx (j + 1);
                Lwt.return a))
        (fun exn ->
           with_transaction'
             (fun tx ->
                Lwt.return (_set_j store tx (j + 1))) >>= fun () ->
           Lwt.fail exn) in
    let _catch_update_in_tx f g =
      Lwt.catch
        (fun () -> update_in_tx f)
        g in
    let update_in_tx_with_not_found notfound_msg f =
      _catch_update_in_tx
        (fun tx -> f tx)
        (function
          | Key_not_found key ->
            let rc = Arakoon_exc.E_NOT_FOUND
            and msg = key in
            Lwt.return (Update_fail (rc,msg))
          | Not_found ->
            let rc = Arakoon_exc.E_NOT_FOUND
            and msg = notfound_msg in
            Lwt.return (Update_fail(rc, msg))
          | Arakoon_exc.Exception(rc, msg) when rc = Arakoon_exc.E_ASSERTION_FAILED ->
            Lwt.return (Update_fail(rc,msg))
          | e -> Lwt.fail e)
    in
    let do_one update =
      match update with
        | Update.Set(key,value) ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.MasterSet (m, lease) ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.Delete(key) ->
          update_in_tx_with_not_found key (fun tx -> _do_one update tx)
        | Update.DeletePrefix prefix ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.TestAndSet(key,expected,wanted)->
          update_in_tx_with_not_found key (fun tx -> _do_one update tx)
        | Update.UserFunction(name,po) ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.Sequence updates
        | Update.SyncedSequence updates ->
          update_in_tx_with_not_found "Not_found" (fun tx -> _do_one update tx)
        | Update.SetInterval interval ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.SetRouting routing ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.SetRoutingDelta (left, sep, right) ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.Nop ->
          Lwt.return (Ok None)
        | Update.Assert(k,vo) ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.Assert_exists(k) ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.AdminSet(k,vo) ->
          update_in_tx (fun tx -> _do_one update tx)
        | Update.Replace(k, wanted) ->
          update_in_tx (fun tx -> _do_one update tx)

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

  let _insert_updates store (us: Update.t list) kt =
    let f u = _insert_update store u kt in
    Lwt_list.map_s f us

  let _insert_value store (value:Value.t) kt =
    let updates = Value.updates_from_value value in
    let j = _get_j store in
    let rec skip n l =
      if n = 0
      then l
      else
        if l = [] then failwith "need to skip more updates than present in this paxos value"
        else skip (n-1) (List.tl l)
    in
    let updates' = skip j updates in
    begin
      if j > 0
      then Logger.debug_f_ "skipped %i updates" j
      else Lwt.return ()
    end
    >>= fun () ->
    _insert_updates store updates' kt >>= fun (urs:update_result list) ->
    _with_transaction store kt (fun tx -> _incr_i store tx) >>= fun () ->
    let prepend_oks n l =
      let rec inner l = function
        | 0 -> l
        | n -> inner (Ok None :: l) (n-1) in
      inner l n in
    Lwt.return (prepend_oks j urs)


  let safe_insert_value store (i:Sn.t) value =
    _wrap_exception store "safe_insert_value" Server.FOOBAR (fun () ->
        let inner f = _with_transaction_lock store (fun key -> f (Key key)) in
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
          if quiesced store
          then
            begin
              incr_i store >>= fun () ->
              Lwt.return [Ok None]
            end
          else
            begin
              _insert_value store value kt
            end
        in
        inner t)


  let on_consensus store (v,n,i) =
    _wrap_exception store "on_consensus" Server.FOOBAR (fun () ->
        Logger.debug_f_ "on_consensus=> local_store n=%s i=%s"
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
        if quiesced store
        then
          begin
            begin
              match v with
                | Value.Vm (m, ls) -> set_master_no_inc store m ls
                | _ -> Lwt.return ()
            end >>= fun () ->
            incr_i store >>= fun () ->
            Lwt.return [Ok None]
          end
        else
          _with_transaction_lock store (fun key -> _insert_value store v (Key key)))

  let get_succ_store_i store = _new_i (consensus_i store)

  let get_catchup_start_i = get_succ_store_i


end

let make_store_module (type ss) (module S : Simple_store with type t = ss) =
  (module Make(S) : STORE with type ss = ss)
