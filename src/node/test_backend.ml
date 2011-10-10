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

open Lwt
open Lwt_log
open Backend
open Statistics
open Update
open Common

module StringMap = Map.Make(String);;


let one_ f first finc last linc max (k:string) (v:string) acc =
  let count = fst acc and a = snd acc in
  if count = max then (count,a)
  else
    let fop = if finc then (>=) else (>)
    and lop = if linc then (<=) else (<)
    in
    let fok = match first with
      | None -> true
      | Some xf -> fop k xf
    and lok = match last with
      | None -> true
      | Some xl -> lop k xl
    in
    if fok && lok then
      (count+1, (f k v)::a)
    else (count, a)

let range_entries_ kv first finc last linc max =
  let one' = one_ (fun k v -> (k,v)) first finc last linc max in
  let _,entries = StringMap.fold one' kv (0,[]) in
  entries

let range_ kv first finc last linc max =
  let one' = one_ (fun k _v -> k) first finc last linc max in
  let _,entries = StringMap.fold one' kv (0,[]) in
  entries

class test_backend my_name = object(self:#backend)
  val mutable _kv = StringMap.empty

  method hello (client_id:string) (cluster_id:string) = 
    let r = 
      match cluster_id with
	| "sweety" -> (0l,    "test_backend.0.0.0")
	| _ ->        (0x06l, Printf.sprintf "I'm not your %s" cluster_id)
    in
    Lwt.return r

  method exists ~allow_dirty (key:string) =
    Lwt.return (StringMap.mem key _kv)

  method set (key:string) (value:string) =
    _kv <- StringMap.add key value _kv;
    Lwt.return ()

  method aSSert ~allow_dirty (key:string) (vo: string option) =
    Lwt_log.debug_f "test_backend :: aSSert %s" key >>= fun () ->
    let ok = match vo with
      | None -> StringMap.mem key _kv = false
      | Some v -> StringMap.find key _kv = v
    in
    if not ok 
    then 
      let rc = Arakoon_exc.E_ASSERTION_FAILED
      and msg = Printf.sprintf "assert %s %S" key ("XXX") 
      in Lwt.fail (Arakoon_exc.Exception (rc, msg))
    else Lwt.return ()


  method get ~allow_dirty (key:string) =
    let value = StringMap.find key _kv in
    Lwt.return value

  method delete (key:string) =
    _kv <- StringMap.remove key _kv;
    Lwt.return ()

  method last_entries (i:Sn.t) f =
    (* TODO *)
    Lwt.return ()

  method test_and_set (key:string) (expected: string option) (wanted:string option) =
    Lwt.return wanted

  method multi_get ~allow_dirty (keys: string list) = 
    let values = List.fold_left 
      (fun acc k -> 
	let v = StringMap.find k _kv in
	(v ::acc))
      [] keys
    in 
    Lwt.return values
		    
  method range_entries ~allow_dirty (first:string option) (finc:bool)
    (last:string option) (linc:bool) (max:int) =
    let x = range_entries_ _kv first finc last linc max in
    info_f "range_entries: found %d entries" (List.length x) >>= fun () ->
    Lwt.return x

  method range ~allow_dirty (first:string option) (finc:bool)
    (last:string option) (linc:bool) (max:int) =
    let x = range_ _kv first finc last linc max in
    info_f "range: found %d entries" (List.length x) >>= fun () ->
    Lwt.return x

  method prefix_keys ~allow_dirty (prefix:string) (max:int) =
    let reg = "^" ^ prefix in
    let keys = StringMap.fold
      (fun k v a ->
	if (Str.string_match (Str.regexp reg) k 0)
	then k::a
	else a
      ) _kv []
    in Lwt.return keys

  method who_master () = Lwt.return (Some my_name)

  method sequence (updates:Update.t list) =
    info_f "test_backend::sequence" >>= fun () ->
    let do_one = function
      | Update.Set (k,v) -> self # set k v
      | Update.Delete k  -> self # delete k
      | _ -> Llio.lwt_failfmt "Sequence only supports SET & DELETE"
    in
    Lwt_list.iter_s do_one updates
    
  method witness name i = Lwt.return ()

  method last_witnessed name = Sn.start

  method expect_progress_possible () = Lwt.return false

  method get_statistics () = Statistics.create() 

  method clear_most_statistics () = ()

  method check ~cluster_id = 
    let r = my_name = cluster_id in
    Lwt.return r

  method collapse n cb' cb = 
    let rec loop i = 
      if i = 0 
      then Lwt.return ()
      else 
	cb () >>= fun () ->
        loop (i-1)
    in
    loop n
    
  method get_key_count () =
    let inc key value size =
      Int64.succ size
    in
    Lwt.return (StringMap.fold inc _kv 0L)

  method get_db s = Lwt.return ()
      
end
