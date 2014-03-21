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



open Simple_store
open Lwt

module StringMap = Map.Make(String)

let default_max_entries = 100
let default_max_size = 100_000

let max_entries = ref default_max_entries
let max_size = ref default_max_size

module Batched_store = functor (S : Extended_simple_store) ->
struct
  type t = {
    s : S.t;
    _tx_cache : (string, string option) Hashtbl.t;

    mutable _with_complex_query : bool;

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
      _tx_cache = Hashtbl.create !max_entries;

      _with_complex_query = false;
      _tx_lock = Lwt_mutex.create ();
      _tx = None;

      _ls_tx = None;

      _entries = 0;
      _size = 0;
    }

  let _apply_vo_to_local_store s ls_tx k vo =
    s._entries <- s._entries + 1;
    match vo with
      | Some v ->
        s._size <- s._size + String.length v;
        S.set s.s ls_tx k v
      | None ->
        try
          S.delete s.s ls_tx k
        with Not_found -> ()

  let _get_ls_tx s =
    match s._ls_tx with
      | None ->
        let ls_tx = S._tranbegin s.s in
        s._ls_tx <- Some ls_tx;
        ls_tx
      | Some ls_tx ->
        ls_tx

  let _track_or_apply_vo s k vo =
    if s._with_complex_query
    then
      _apply_vo_to_local_store s (_get_ls_tx s) k vo
    else
      Hashtbl.replace s._tx_cache k vo

  let _apply_cache_to_local_store s =
    if Hashtbl.length s._tx_cache > 0
    then
      begin
        let ls_tx = _get_ls_tx s in
        let () = Hashtbl.iter
                   (fun k vo -> _apply_vo_to_local_store s ls_tx k vo)
                   s._tx_cache in
        Hashtbl.reset s._tx_cache
      end

  let _commit_ls_tx_if_any s =
    match s._ls_tx with
      | None -> ()
      | Some ls_tx ->
        S._trancommit s.s;
        s._ls_tx <- None;
        s._entries <- 0;
        s._size <- 0

  let _with_complex_query ~allow_in_transaction s =
    if s._tx <> None || not allow_in_transaction
    then
      begin
        _commit_ls_tx_if_any s;
        s._with_complex_query <- true;
        _apply_cache_to_local_store s
      end

  let with_transaction s f =
    Lwt_mutex.with_lock s._tx_lock (fun () ->
        let tx = new transaction in
        s._tx <- Some tx;
        s._with_complex_query <- false;
        Lwt.finalize
          (fun () ->
             f tx >>= fun r ->
             _apply_cache_to_local_store s;

             begin
               if (s._entries >= !max_entries) || (s._size >= !max_size)
               then
                 begin
                   Logger.debug_f_ "Batched_store, commiting changes to local_store (_entries=%i, _size=%i)" s._entries s._size >>= fun () ->
                   _commit_ls_tx_if_any s;
                   Lwt.return ()
                 end
               else
                 Lwt.return ()
             end >>= fun () ->

             Lwt.return r)
          (fun () ->
             s._tx <- None;
             Hashtbl.reset s._tx_cache;
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

  let _with_key_in_cache s k match' else' =
    match _find s._tx_cache k with
      | Some v -> match' v
      | None -> else' ()

  let exists s k =
    _with_key_in_cache s k
      (function
        | None -> false
        | Some _ -> true)
      (fun () -> S.exists s.s k)

  let get s k =
    _with_key_in_cache s k
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
    _with_complex_query ~allow_in_transaction:false s;
    S.delete_prefix s.s (_get_ls_tx s)  prefix

  let flush s =
    _commit_ls_tx_if_any s;
    Lwt.return ()

  let close s flush =
    begin
      if flush
      then
        _commit_ls_tx_if_any s
      else
        begin
          match s._ls_tx with
            | None -> ()
            | Some _ ->
              S._tranabort s.s;
              s._ls_tx <- None
        end
    end;
    S.close s.s flush

  let reopen s =
    _commit_ls_tx_if_any s;
    S.reopen s.s

  let get_location s =
    S.get_location s.s

  let relocate s =
    _commit_ls_tx_if_any s;
    S.relocate s.s

  let get_key_count s =
    _with_complex_query ~allow_in_transaction:false s;
    S.get_key_count s.s

  let optimize s =
    _commit_ls_tx_if_any s;
    S.optimize s.s

  let defrag s =
    _commit_ls_tx_if_any s;
    S.defrag s.s

  let copy_store s =
    _commit_ls_tx_if_any s;
    S.copy_store s.s

  let copy_store2 =
    S.copy_store2

  type cursor = S.cursor

  let with_cursor s f =
    _with_complex_query ~allow_in_transaction:true s;
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

module Local_store = Batched_store(Leveldb_store.LevelDBStore)
