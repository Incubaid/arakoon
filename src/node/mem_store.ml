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
open Simple_store
open Lwt
open Log_extra
open Update
open Routing

module StringMap = Test_backend.StringMap

type t = { mutable kv : string StringMap.t;
           mutable _tx : transaction option;
           name : string;
         }

module Cursor = struct
  type cursor = (string StringMap.zipper option ref * t)

  let cur_last (zip, ls) =
    zip := StringMap.cur_last ls.kv;
    !zip <> None

  let with_initialized zip f =
    match !zip with
    | None -> failwith "invalid cursor"
    | Some z -> f z

  let cur_get (zip, ls) =
    with_initialized
      zip
      (fun zip ->
       StringMap.cur_get zip)

  let cur_get_key cur =
    let k, _ = cur_get cur in
    k

  let cur_prev (zip, ls) =
    match !zip with
    | None -> false
    | Some z ->
       zip := StringMap.cur_prev z;
       !zip <> None

  let cur_next (zip, ls) =
    match !zip with
    | None -> false
    | Some z ->
       zip := StringMap.cur_next z;
       !zip <> None

  let cur_jump (zip, ls) key =
    zip := StringMap.cur_jump key ls.kv;
    !zip <> None
end

include Cursor

let with_cursor ls f =
  let zip = ref None in
  f (zip, ls)

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

let delete_prefix ms tx prefix =
  _verify_tx ms tx;
  let module CS = Extended_cursor_store(Cursor) in
  let count, () =
    with_cursor
      ms
      (fun cur ->
       CS.fold_range cur prefix true (next_prefix prefix) false
                     (-1)
                     (fun cur () ->
                      delete ms tx (cur_get_key cur))
                     ()) in
  count

let set ms tx key value =
  _verify_tx ms tx;
  ms.kv <- StringMap.add key value ms.kv

let optimize ms quiesced = Lwt.return ()
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

let copy_store ms networkClient oc = failwith "copy_store not supported"
let copy_store2 old_location new_location overwrite = Lwt.return ()

let relocate new_location = failwith "Memstore.relocation not implemented"

let get_fringe ms boundary direction =
  Logger.debug_f_ "mem_store :: get_border_range %s" (Log_extra.string_option2s boundary) >>= fun () ->
  let cmp =
    begin
      match direction, boundary with
        | Routing.UPPER_BOUND, Some b -> (fun k -> b < k )
        | Routing.LOWER_BOUND, Some b -> (fun k -> b >= k)
        | _ , None -> (fun k -> true)
    end
  in
  let all = StringMap.fold
              (fun k v acc ->
                 if cmp k
                 then (k,v)::acc
                 else acc)
              ms.kv []
  in
  Lwt.return all

let make_store ~lcnum ~ncnum read_only db_name =
  Lwt.return { kv = StringMap.empty;
               _tx = None;
               name = db_name; }

let copy_store old_location new_location overwrite =
  Lwt.return ()
