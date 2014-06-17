(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

open Std
open Simple_store
open Lwt
open LevelDB

(*
TODO

- with batch semantics ... -> get/exist/delete should keep in mind changes already in 'batch'
- copy/relocate store methods (work with directories)
- open db RO

 *)

module LevelDBStore =(
  struct
    type t = {
      mutable db: LevelDB.db ;
      mutable location : string;
      mutable _tx: (transaction * LevelDB.writebatch) option;
    }


    let _tranbegin t =
      let tx = new transaction in
      let batch = LevelDB.Batch.make () in
      t._tx <- Some (tx,batch);
      tx

    let _trancommit t =
      match t._tx with
      | None -> failwith "???"
      | Some (_,batch) ->
         LevelDB.Batch.write t.db batch

    let _tranabort t =
      match t._tx with
      | None -> failwith "not in transaction"
      | Some (tx,batch) -> t._tx <- None

    let make_store ~lcnum ~ncnum b filename =
      let db = LevelDB.open_db filename in
      Lwt.return { db;location = filename; _tx = None}

    let with_transaction t f =
      let tx = _tranbegin t in
      Lwt.finalize
        (fun () ->
         f tx >>= fun a ->
         _trancommit t;
         Lwt.return a
        )
        (fun () -> t._tx <- None; Lwt.return ())

    let __with_batch t f =
      match t._tx with
      | None -> failwith "no batch"
      | Some (_,batch) ->
         f batch

    let get t key =
      LevelDB.get_exn t.db key

    let exists t key = LevelDB.mem t.db key

    let set t tx key value =
      __with_batch t (fun batch -> LevelDB.Batch.put batch key value)

    let delete t tx key =
      ignore (get t key); (* to throw Not_found when needed *)
      __with_batch t (fun batch -> LevelDB.Batch.delete batch key)

    let sync t =
      ()

    let close t flush =
      LevelDB.close t.db;
      Lwt.return ()

    let get_key_count t = failwith "get_key_count not supported"

    let copy_store t b oc = Lwt.fail (Failure "copy_store")
    let copy_store2 x y b = Logger.warning_f_ "no copy store2"
    let defrag t = Logger.warning_f_ "no defrag"
    let optimize t quisced = Logger.warning_f_ "no optimize"
    let relocate t s = Lwt.fail (Failure "relocate")
    let get_location t = t.location
    let flush t = Logger.warning_f_ "no flushing"
    let reopen t when_closed ro =
      close t true >>= fun () ->
      when_closed () >>= fun () ->
      t.db <- LevelDB.open_db t.location; (* TODO use ro *)
      Lwt.return ()

    module Cursor = struct
      type cursor = LevelDB.iterator

      let with_cursor t f =
        let snapshot = Snapshot.make t.db in
        let iterator = Snapshot.iterator snapshot in
        try
          let r = f iterator in
          Iterator.close iterator;
          Snapshot.release snapshot;
          r
        with
        | e ->
           Iterator.close iterator;
           Snapshot.release snapshot;
           raise e

      let cur_last cur =
        Iterator.seek_to_last cur;
        Iterator.valid cur

      let cur_jump cur ?(direction=Right) key =
        match direction with
        | Right ->
           Iterator.seek cur key 0 (String.length key);
           Iterator.valid cur
        | Left ->
           Iterator.seek cur key 0 (String.length key);
           if Iterator.valid cur
           then
             begin
               if String.(=:) key (Iterator.get_key cur)
               then
                 true
               else
                 begin
                   Iterator.prev cur;
                   Iterator.valid cur
                 end
             end
           else
             cur_last cur

      let cur_next cur =
        Iterator.next cur;
        Iterator.valid cur

      let cur_prev cur =
        Iterator.prev cur;
        Iterator.valid cur

      let cur_get_key cur =
        Iterator.get_key cur

      let cur_get_value cur =
        Iterator.get_value cur

      let cur_get cur =
        (cur_get_key cur, cur_get_value cur)
    end

    include Cursor

    module CS = Extended_cursor_store(Cursor)

    let delete_prefix t tx prefix =
      let count, () =
        with_cursor
          t
          (fun cur ->
           CS.fold_range cur prefix true (next_prefix prefix) false
                         (-1)
                         (fun cur k _ () ->
                          delete t tx k)
                         ()) in
      count

end : Extended_simple_store)

let default_max_entries = 100
let default_max_size = 100_000

let max_entries = ref default_max_entries
let max_size = ref default_max_size


module Batched_store = functor (S : Extended_simple_store) ->
struct
  type t = {
    s : S.t;
    _cache : (string, string option) Hashtbl.t;
    _current_tx_cache : (string, string option) Hashtbl.t;

    _tx_lock : Lwt_mutex.t;
    mutable _tx : transaction option;

    mutable _ls_tx : transaction option;

    mutable _entries : int;
    mutable _size : int;
  }

  let make_store ~lcnum ~ncnum b s =
    S.make_store ~lcnum ~ncnum b s >>= fun s ->
    Lwt.return {
      s;
      _cache = Hashtbl.create !max_entries;
      _current_tx_cache = Hashtbl.create 10;

      _tx_lock = Lwt_mutex.create ();
      _tx = None;

      _ls_tx = None;

      _entries = 0;
      _size = 0;
    }

  let _apply_vo_to_local_store s ls_tx k vo =
    match vo with
      | Some v -> S.set s ls_tx k v
      | None ->
        if S.exists s k
        then
          S.delete s ls_tx k

  let _apply_vos_to_local_store s ls_tx m =
    Hashtbl.iter
      (fun k vo -> _apply_vo_to_local_store s ls_tx k vo)
      m

  let _track_or_apply_vo s k vo =
    match s._ls_tx with
      | None ->
        Hashtbl.replace s._current_tx_cache k vo
      | Some ls_tx ->
        _apply_vo_to_local_store s.s ls_tx k vo

  let _sync_cache_to_local_store s =
    if Hashtbl.length s._cache <> 0
    then
      begin
        let ls_tx = S._tranbegin s.s in
        s._ls_tx <- Some ls_tx;

        (* this should always succeed *)
        _apply_vos_to_local_store s.s ls_tx s._cache;
        S._trancommit s.s;

        Hashtbl.reset s._cache;
        s._ls_tx <- None;
        s._entries <- 0;
        s._size <- 0
      end

  let _sync_and_start_transaction_if_needed s =
    if s._tx = None
    then
      _sync_cache_to_local_store s
    else
      (* we're asked to sync from within a Batched_store transaction *)
    if s._ls_tx = None (* start a S transaction should none be initiated so far *)
    then
      begin
        _sync_cache_to_local_store s;
        let ls_tx = S._tranbegin s.s in
        s._ls_tx <- Some ls_tx;
        _apply_vos_to_local_store s.s ls_tx s._current_tx_cache
      end

  let with_transaction s f =
    Lwt_mutex.with_lock s._tx_lock (fun () ->
        let tx = new transaction in
        s._tx <- Some tx;
        Lwt.finalize
          (fun () ->
             f tx >>= fun r ->
             let () =
               match s._ls_tx with
                 | None ->
                   (* transaction succeeded and no new transaction was started
(to handle the more difficult cases),
so let's apply the changes accumulated in _current_tx_cache to _cache *)
                   Hashtbl.iter
                     (fun k vo ->
                        Hashtbl.replace s._cache k vo;

                        s._entries <- s._entries + 1;
                        (match vo with
                           | None -> ()
                           | Some v -> s._size <- s._size + String.length v);
                        s._size <- s._size + String.length k)
                     s._current_tx_cache
                 | Some ls_tx ->
                   (* a ls_transaction was started while in this batched_store transaction,
now is the time to finish that transaction *)
                   S._trancommit s.s;
                   s._ls_tx <- None
             in

             begin
               if (s._entries >= !max_entries) || (s._size >= !max_size)
               then
                 begin
                   Logger.debug_f_ "Batched_store, synching cache to local_store (_entries=%i, _size=%i)" s._entries s._size >>= fun () ->
                   _sync_cache_to_local_store s;
                   Lwt.return ()
                 end
               else
                 Lwt.return ()
             end >>= fun () ->

             Lwt.return r)
          (fun () ->
             let () = match s._ls_tx with
               | None -> ()
               | Some ls_tx ->
                 (* a ls_transaction was started while in this batched_store transaction,
got an exception so aborting the transaction *)
                 S._tranabort s.s;
                 s._ls_tx <- None
             in

             s._tx <- None;
             Hashtbl.reset s._current_tx_cache;
             Lwt.return ()))

  let _verify_tx s tx =
    match s._tx with
      | None -> failwith "not in a batched store transaction"
      | Some tx' ->
        if tx != tx'
        then failwith "the provided transaction is not the current transaction of the batched store"

  let _find c k =
    try Some (Hashtbl.find c k)
    with Not_found -> None

  let _with_key_in_caches s k match' else' =
    match _find s._current_tx_cache k with
      | Some v -> match' v
      | None ->
        begin
          match _find s._cache k with
            | Some v -> match' v
            | None -> else' ()
        end

  let exists s k =
    _with_key_in_caches s k
      (function
        | None -> false
        | Some _ -> true)
      (fun () -> S.exists s.s k)

  let get s k =
    _with_key_in_caches s k
      (function
        | None -> raise Not_found
        | Some v -> v)
      (fun () -> S.get s.s k)

  let set s tx k v =
    _verify_tx s tx;
    _track_or_apply_vo s k (Some v)

  let delete s tx k =
    _verify_tx s tx;
    if exists s k
    then
      _track_or_apply_vo s k None
    else
      raise Not_found

  let delete_prefix s tx prefix =
    _verify_tx s tx;
    _sync_and_start_transaction_if_needed s;
    match s._ls_tx with
      | None -> failwith "batched_store s._ls_tx is None"
      | Some ls_tx -> S.delete_prefix s.s ls_tx prefix

  let flush s =
    _sync_and_start_transaction_if_needed s;
    Lwt.return ()

  let sync s =
    _sync_and_start_transaction_if_needed s;
    S.sync s.s

  let close s flush =
    if flush
    then
      _sync_and_start_transaction_if_needed s;
    S.close s.s flush

  let reopen s =
    _sync_and_start_transaction_if_needed s;
    S.reopen s.s

  let get_location s =
    S.get_location s.s

  let relocate s =
    _sync_and_start_transaction_if_needed s;
    S.relocate s.s

  let get_key_count s =
    _sync_and_start_transaction_if_needed s;
    S.get_key_count s.s

  let optimize s =
    _sync_and_start_transaction_if_needed s;
    S.optimize s.s

  let defrag s =
    _sync_and_start_transaction_if_needed s;
    S.defrag s.s

  let copy_store s =
    _sync_and_start_transaction_if_needed s;
    S.copy_store s.s

  let copy_store2 =
    S.copy_store2

  type cursor = S.cursor

  let with_cursor s f =
    _sync_and_start_transaction_if_needed s;
    S.with_cursor s.s (fun cur -> f cur)

  let cur_last =
    S.cur_last

  let cur_get =
    S.cur_get

  let cur_get_key =
    S.cur_get_key

  let cur_get_value =
    S.cur_get_value

  let cur_prev =
    S.cur_prev

  let cur_next =
    S.cur_next

  let cur_jump =
    S.cur_jump
end

module Level_store = Batched_store(LevelDBStore)
