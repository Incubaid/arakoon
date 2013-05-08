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
open Log_extra
open Update
open Interval
open Routing

module StringMap = Map.Make(String);;

let try_lwt_ f = Lwt.catch (fun () -> Lwt.return (f ())) (fun exn -> Lwt.fail exn)

class mem_store db_name =
  let now64 () = Int64.of_float (Unix.gettimeofday ())
  in

object (self: #simple_store)

  val mutable i = None
  val mutable kv = StringMap.empty
  val mutable master = None
  val mutable _interval = Interval.max
  val mutable _routing = None
  val mutable _tx = None
  val mutable _tx_lock = None
  val _tx_lock_mutex = Lwt_mutex.create ()

  method is_closed () =
    false

  method with_transaction_lock f =
    Lwt_mutex.with_lock _tx_lock_mutex (fun () ->
      Lwt.finalize
        (fun () ->
          let txl = new transaction_lock in
          _tx_lock <- Some txl;
          f txl)
        (fun () -> _tx_lock <- None; Lwt.return ()))

  method with_transaction ?(key=None) f =
    let matched_locks = match _tx_lock, key with
      | None, None -> true
      | Some txl, Some txl' -> txl == txl'
      | _ -> false in
    if not matched_locks
    then failwith "transaction locks do not match";
    let tx = new transaction in
    _tx <- Some tx;
    let current_i = i in
    let current_kv = kv in
    Lwt.catch
      (fun () ->
        Lwt.finalize
          (fun () -> f tx)
          (fun () -> _tx <- None; Lwt.return ()))
      (fun exn ->
        i <- current_i;
        kv <- current_kv;
        Lwt.fail exn)

  method incr_i tx =
    Lwt.return (self # _incr_i ())

  method _incr_i () =
    let i2 = match i with
    | None -> Some 0L
    | Some i' -> Some ( Sn.succ i' )
    in
    let () = i <- i2 in
    ()

  method _set_i x =
    i <- Some x

  method exists key =
    try_lwt_ (fun () -> StringMap.mem key kv)

  method get key =
    StringMap.find key kv

  method range prefix first finc last linc max =
    let keys = Test_backend.range_ kv (_f prefix first) finc (_l prefix last) linc max in
    filter_keys_list keys

  method range_entries prefix first finc last linc max =
    let entries = Test_backend.range_entries_ kv (_f prefix first) finc (_l prefix last) linc max in
    filter_entries_list entries

  method rev_range_entries prefix first finc last linc max =
    let entries = Test_backend.rev_range_entries_ kv (_f prefix first) finc (_l prefix last) linc max in
    filter_entries_list entries

  method prefix_keys prefix max =
    let reg = "^" ^ prefix in
    let keys = StringMap.fold
      (fun k v a ->
(* TODO this is buggy -> what if prefix contains special regex chars? *)
	if (Str.string_match (Str.regexp reg) k 0)
	then k::a
	else a
      ) kv []
    in Lwt.return (filter_keys_list keys)

  method set tx key value =
    kv <- StringMap.add key value kv

  method set_master tx master' l =
    let () = master <- Some (master', now64()) in
    Lwt.return ()

  method set_master_no_inc master' l =
    let () = master <- Some (master', now64()) in
    Lwt.return ()


  method quiesce () = Lwt.return ()

  method unquiesce () = Lwt.return ()

  method quiesced () = false

  method optimize () = Lwt.return ()
  method defrag () = Lwt.return ()

  method who_master () = master

  method private delete_no_incr ?(_pf=__prefix) key =
    if StringMap.mem key kv then
      begin
	    Lwt_log.ign_debug_f "%S exists" key;
	    kv <- StringMap.remove key kv
      end
    else
      begin
	    Lwt_log.ign_debug "going to fail";
	    raise (Key_not_found key)
      end

  method delete tx key =
    Lwt_log.ign_debug_f "mem_store # delete %S" key;
    self # delete_no_incr key

  method consensus_i () = i

  method close () = Lwt.return ()

  method reopen when_closed = Lwt.return ()

  method get_location () = failwith "not supported"

  method set_interval tx iv =
    Lwt_log.debug_f "set_interval %s" (Interval.to_string iv) >>= fun () ->
    _interval <- iv;
    Lwt.return ()

  method get_interval () =
    Lwt_log.ign_debug "get_interval";
    _interval

  method get_routing () =
    match _routing with
      | None -> Lwt.fail Not_found
      | Some r -> Lwt.return r

  method set_routing tx r =
    _routing <- Some r;
    Lwt.return ()

  method set_routing_delta tx left sep right =
    match _routing with
      | None -> failwith "Cannot update non-existing routing"
      | Some r ->
        begin
          let new_r = Routing.change r left sep right in
          Lwt.return ( _routing <- Some new_r )
        end

  method get_key_count () =
    let inc key value size =
      Int64.succ size
    in
    Lwt.return (StringMap.fold inc kv 0L)

  method copy_store ?(_networkClient=true) oc = failwith "copy_store not supported"

  method relocate new_location = failwith "Memstore.relocation not implemented"

  method get_fringe boundary direction =
    Lwt_log.debug_f "mem_store :: get_border_range %s" (Log_extra.string_option2s boundary) >>= fun () ->
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
      kv []
    in
    Lwt.return all

  method delete_prefix tx ?(_pf=__prefix) prefix = Lwt.return 0
    
end

let make_mem_store ?(read_only=false) db_name =
  let store = new mem_store db_name in
  let store2 = (store :> simple_store) in
  Lwt.return { s = store2 }

let copy_store old_location new_location overwrite =
  Lwt.return ()


