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

module StringMap = Map.Make(String)

type t = { mutable kv : string StringMap.t;
           mutable _tx : transaction option;
           name : string;
         }

module Cursor = struct
  open StringMap
  type cursor = string Cursor.t

  let cur_last t =
    Cursor.last t

  let cur_get t =
    Cursor.get t

  let cur_get_key t =
    fst (cur_get t)

  let cur_get_value t =
    snd (cur_get t)

  let cur_prev t =
    Cursor.prev t

  let cur_next t =
    Cursor.next t

  let cur_jump t ?direction key =
    Cursor.jump ?dir:direction key t
end

include Cursor

let with_cursor ls f =
  StringMap.Cursor.with_cursor ls.kv f

let with_transaction ms f =
  let tx = new transaction in
  ms._tx <- Some tx;
  let current_kv = ms.kv in
  Lwt.catch
    (fun () ->
       Lwt.finalize
         (fun () -> f tx)
         (fun () -> ms._tx <- None; Lwt.return ()))
    (fun exn ->
       ms.kv <- current_kv;
       Lwt.fail exn)

let _verify_tx ms tx =
  match ms._tx with
    | None -> failwith "not in a transaction"
    | Some tx' ->
      if tx != tx'
      then failwith "the provided transaction is not the current transaction of the store"

let exists ms key =
  StringMap.mem key ms.kv

let get ms key =
  StringMap.find key ms.kv

let delete ms tx key =
  _verify_tx ms tx;
  if StringMap.mem key ms.kv then
    ms.kv <- StringMap.remove key ms.kv
  else
    raise (Key_not_found key)

module CS = Extended_cursor_store(Cursor)

let delete_prefix ms tx prefix =
  _verify_tx ms tx;
  let count, () =
    with_cursor
      ms
      (fun cur ->
       CS.fold_range cur prefix true (next_prefix prefix) false
                     (-1)
                     (fun cur k _ () ->
                      delete ms tx k)
                     ()) in
  count


let range ms first finc last linc max =
  let count, keys =
    with_cursor
      ms
      (fun cur ->
       CS.fold_range cur
                     first finc last linc
                     max
                     (fun cur k _ acc ->
                      k :: acc)
                     []) in
  Array.of_list keys

let set ms tx key value =
  _verify_tx ms tx;
  ms.kv <- StringMap.add key value ms.kv

let optimize ms ~quiesced ~stop = Lwt.return true
let defrag ms = Lwt.return ()

let flush ms = Lwt.return ()
let close ms flush = Lwt.return ()

let reopen ms when_closed quiesced = Lwt.return ()

let get_location ms = ms.name

let get_key_count ms =
  let inc key value size =
    Int64.succ size
  in
  Lwt.return (StringMap.fold inc ms.kv 0L)

let copy_store2 old_location new_location overwrite = Lwt.return ()

let relocate new_location = failwith "Memstore.relocation not implemented"

let make_store ~lcnum ~ncnum read_only db_name =
  Lwt.return { kv = StringMap.empty;
               _tx = None;
               name = db_name; }

let copy_store old_location new_location overwrite =
  Lwt.return ()
