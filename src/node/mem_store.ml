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

open Store
open Lwt
open Routing

module StringMap = Map.Make(String);;

type t = { mutable kv : string StringMap.t;
           mutable _tx : transaction option;
           name : string;
         }

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

let range ms prefix first finc last linc max =
    let keys = Test_backend.range_ ms.kv (_f prefix first) finc (_l prefix last) linc max in
    filter_keys_list keys

let range_entries ms prefix first finc last linc max =
    let entries = Test_backend.range_entries_ ms.kv (_f prefix first) finc (_l prefix last) linc max in
    filter_entries_list entries

let rev_range_entries ms prefix first finc last linc max =
    let entries = Test_backend.rev_range_entries_ ms.kv (_f prefix first) finc (_l prefix last) linc max in
    filter_entries_list entries

let prefix_keys ms prefix _max=
    let reg = "^" ^ prefix in
    let keys = StringMap.fold
      (fun k _v a ->
(* TODO this is buggy -> what if prefix contains special regex chars? *)
	if (Str.string_match (Str.regexp reg) k 0)
	then k::a
	else a
      ) ms.kv []
    in filter_keys_list keys

let delete ms tx key =
  _verify_tx ms tx;
  if StringMap.mem key ms.kv then
	ms.kv <- StringMap.remove key ms.kv
  else
	raise (Key_not_found key)

let delete_prefix ms tx prefix =
  _verify_tx ms tx;
  let keys = prefix_keys ms prefix (-1) in
  let () = List.iter (fun k -> delete ms tx k) keys in
  List.length keys

let set ms tx key value =
  _verify_tx ms tx;
  ms.kv <- StringMap.add key value ms.kv

let optimize _ms _quiesced = Lwt.return ()
let defrag _ms = Lwt.return ()

let flush _ms = Lwt.return ()
let close _ms _flush = Lwt.return ()

let reopen _ms _when_closed _quiesced = Lwt.return ()

let get_location ms = ms.name

let get_key_count ms =
    let inc _key _value size =
      Int64.succ size
    in
    Lwt.return (StringMap.fold inc ms.kv 0L)

let copy_store2 _old_location _new_location _overwrite = Lwt.return ()

let relocate _new_location = failwith "Memstore.relocation not implemented"

let get_fringe ms boundary direction =
    Logger.debug_f_ "mem_store :: get_border_range %s" (Log_extra.string_option2s boundary) >>= fun () ->
    let cmp =
      begin
        match direction, boundary with
          | Routing.UPPER_BOUND, Some b -> (fun k -> b < k )
          | Routing.LOWER_BOUND, Some b -> (fun k -> b >= k)
          | _ , None -> (fun _k -> true)
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

let make_store _read_only db_name =
  Lwt.return { kv = StringMap.empty;
               _tx = None;
               name = db_name; }

let copy_store _old_location _new_location _overwrite =
  Lwt.return ()


