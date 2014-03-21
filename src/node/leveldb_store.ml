(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2014 Incubaid BVBA

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
open LevelDB

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
      match LevelDB.get t.db key with
      | None -> raise Not_found
      | Some v ->  v

    let exists t key = LevelDB.mem t.db key

    let set t tx key value =
      __with_batch t (fun batch -> LevelDB.Batch.put batch key value)

    let delete t tx key =
      (* TODO should return not found? *)
      __with_batch t (fun batch -> LevelDB.Batch.delete batch key)

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
