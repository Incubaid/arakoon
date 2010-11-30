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

module StringMap = Map.Make(String);;

let try_lwt_ f = Lwt.catch (fun () -> Lwt.return (f ())) (fun exn -> Lwt.fail exn)

class mem_store db_name =
object (self: #store)

  val mutable i = None
  val mutable kv = StringMap.empty
  val mutable master = None

  method _incr_i () =
    let i2 = match i with
    | None -> Some 0L
    | Some i' -> Some ( Sn.succ i' )
    in
    let () = i <- i2 in
    ()

  method exists key =
    try_lwt_ (fun () -> StringMap.mem key kv)

  method get key =
    try_lwt_ (fun () -> StringMap.find key kv)

  method multi_get keys =
    let values = List.fold_left 
      (fun acc key -> let value = StringMap.find key kv in value :: acc)
      [] keys
    in 
    Lwt.return (List.rev values)

  method range first finc last linc max =
    let keys = Test_backend.range_ kv first finc last linc max in
    Lwt.return keys

  method range_entries first finc last linc max =
    let entries = Test_backend.range_entries_ kv first finc last linc max in
    Lwt.return entries

  method prefix_keys prefix max =
    let reg = "^" ^ prefix in
    let keys = StringMap.fold
      (fun k v a ->
	if (Str.string_match (Str.regexp reg) k 0)
	then k::a
	else a
      ) kv []
    in Lwt.return keys

  method set key value =
    let () = self # _incr_i () in
    let () = kv <- StringMap.add key value kv in
    Lwt.return ()

  method set_master master' l =
    let () = self # _incr_i () in
    let () = master <- Some (master',l) in
    Lwt.return ()

  method set_master_no_inc master' l =
    let () = master <- Some (master',l) in
    Lwt.return ()

  method who_master () =
    Lwt.return master

  method delete key =
    Lwt_log.debug_f "mem_store # delete %S" key >>= fun () ->
    if StringMap.mem key kv then
      begin
	Lwt_log.debug_f "%S exists" key >>= fun () ->
	let () = self # _incr_i () in
	let () = kv <- StringMap.remove key kv in
	Lwt.return ()
      end
    else
      begin
	Lwt_log.debug "going to fail" >>= fun () ->
	Lwt.fail (Key_not_found key)
      end

  method test_and_set key expected wanted =
    Lwt.catch 
      (fun () ->
	self # get key >>= fun res -> Lwt.return (Some res))
      (function 
	| Not_found -> Lwt.return None 
	| exn -> Lwt.fail exn)
    >>= fun existing ->
    if existing <> expected
    then Lwt.return existing
    else
      begin
	(match wanted with
	  | None -> self # delete key 
	  | Some wanted_s -> self # set key wanted_s)
	>>= fun () -> Lwt.return wanted
      end

  method sequence updates = 
    Lwt_log.info "mem_store :: sequence" >>= fun () ->
    let do_one =  function
      | Update.Set (k,v) -> 
	Lwt_log.debug_f "set # %S %S" k v >>= fun () -> 
	self # set k v
      | Update.Delete k  -> 
	Lwt_log.debug_f "delete # %S" k >>= fun () ->
	self # delete k >>= fun () ->
	Lwt_log.debug "done delete"
      | _ -> Llio.lwt_failfmt "Sequence only supports SET & DELETE"
    in
    let old_kv = kv in
    Lwt.catch
      (fun () -> 
	Lwt_list.iter_s do_one updates)
      (fun exn -> kv <- old_kv; 
	Lwt_log.debug ~exn "mem_store :: sequence failed" >>= fun () ->
	Lwt.fail exn)

  method consensus_i () = Lwt.return i

  method close () = Lwt.return ()
end

let make_mem_store db_name =
  let store = new mem_store db_name in
  let store2 = (store :> store) in
  Lwt.return store2
