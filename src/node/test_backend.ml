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
open Backend
open Statistics
open Update
open Routing
open Common
open Interval

let section = Logger.Section.main

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

let rev_one_ first finc last linc max acc (kv: string * string) =
  let (k,v) = kv in
  let (count,a) = acc in
  if count = max then (count,a)
  else
    let fop = if finc then (<=) else (<) (* reversed *)
    and lop = if linc then (>=) else (>) (* reversed *)
    in
    let fok = match first with
      | None -> true
      | Some xf -> fop k xf
    and lok = match last with
      | None -> true
      | Some xl -> lop k xl
    in
    if fok && lok then
      (count+1, kv::a)
    else (count, a)

let rev_range_entries_ kv first finc last linc max =
  let rev_l = StringMap.fold (fun k v a -> (k,v)::a) kv [] in
  let one' = rev_one_ first finc last linc max in
  let _, entries = List.fold_left one' (0,[]) rev_l in
  entries

let range_ kv first finc last linc max =
  let one' = one_ (fun k _v -> k) first finc last linc max in
  let _,entries = StringMap.fold one' kv (0,[]) in
  entries

class test_backend my_name = object(self:#backend)
  val mutable _kv = StringMap.empty
  val mutable _routing = (None: Routing.t option)
  val mutable _interval = Interval.max

  method optimize_db () =
    Lwt.return () 

  method defrag_db () = Lwt.return ()

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

  method confirm (key:string) (value:string) =
    if StringMap.mem key _kv && StringMap.find key _kv = value then Lwt.return ()
    else self # set key value

  method aSSert ~allow_dirty (key:string) (vo: string option) =
    Logger.debug_f_ "test_backend :: aSSert %s" key >>= fun () ->
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

  method aSSert_exists ~allow_dirty (key:string)=
    Logger.debug_f_ "test_backend :: aSSert_exists %s" key >>= fun () ->
    let ok =
      StringMap.mem key _kv
    in
    if not ok
    then
      let rc = Arakoon_exc.E_ASSERTION_FAILED
      and msg = Printf.sprintf "assert_exists %s %S" key ("XXX")
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

  method user_function name po =
    match name with
      | "reverse" ->
	begin
	  match po with
	    | None -> Lwt.return None
	    | Some s ->
	      let r = ref "" in
	      String.iter (fun c -> r := (String.make 1 c ^ !r)) s;
	      Lwt.return (Some !r)
	end
      | _ -> Lwt.return None

  method multi_get ~allow_dirty (keys: string list) =
    let values = List.fold_left
      (fun acc k ->
	let v = StringMap.find k _kv in
	(v ::acc))
      [] keys
    in
    Lwt.return values


  method multi_get_option ~allow_dirty (keys: string list) =
    let vos = List.fold_left
      (fun acc k ->
        let vo = 
          try Some (StringMap.find k _kv )
          with Not_found -> None
        in
        (vo :: acc))
      [] keys
    in
    Lwt.return vos

  method range_entries ~allow_dirty (first:string option) (finc:bool)
    (last:string option) (linc:bool) (max:int) =
    let x = range_entries_ _kv first finc last linc max in
    Logger.info_f_ "range_entries: found %d entries" (List.length x) >>= fun () ->
    Lwt.return x

  method rev_range_entries ~allow_dirty (first:string option) (finc:bool)
    (last:string option) (linc:bool) (max:int) =
    let x = rev_range_entries_ _kv first finc last linc max in
    Logger.info_f_ "rev_range_entries: found %d entries" (List.length x) >>= fun () ->
    Lwt.return x

  method range ~allow_dirty (first:string option) (finc:bool)
    (last:string option) (linc:bool) (max:int) =
    let x = range_ _kv first finc last linc max in
    Logger.info_f_ "range: found %d entries" (List.length x) >>= fun () ->
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

  method sequence ~sync:bool (updates:Update.t list) =
    Logger.info_f_ "test_backend::sequence" >>= fun () ->
    let do_one = function
      | Update.Set (k,v) -> self # set k v
      | Update.Delete k  -> self # delete k
      | _ -> Llio.lwt_failfmt "Sequence only supports SET & DELETE"
    in
    Lwt_list.iter_s do_one updates

  method witness name i = ()

  method last_witnessed name = Sn.start

  method expect_progress_possible () = Lwt.return false

  method get_statistics () = Statistics.create()
  method clear_most_statistics() = ()

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

  method set_interval interval =
    Logger.debug_f_ "set_interval %s" (Interval.to_string interval) >>= fun () ->
    _interval <- interval; Lwt.return ()
  method get_interval () = Lwt.return _interval


  method get_routing () =
    match _routing with
      | None -> Lwt.fail Not_found
      | Some r -> Lwt.return r

  method set_routing r =
    _routing <- Some r;
    Lwt.return ()

  method set_routing_delta left sep right =
    begin
      match _routing with
        | Some r ->
          let new_r = Routing.change r left sep right in
          Lwt.return (_routing <- Some new_r )
        | None -> failwith "Cannot modify non-existing routing"
    end

  method get_key_count () =
    let inc key value size =
      Int64.succ size
    in
    Lwt.return (StringMap.fold inc _kv 0L)

  method get_db s =
    Lwt.return ()

  method get_fringe boundary direction =
    let cmp =
      begin
        match direction, boundary with
          | Routing.UPPER_BOUND, Some b -> (fun k -> k < b)
          | Routing.LOWER_BOUND, Some b -> (fun k -> k >= b)
          | _ , None -> (fun k -> true)
      end
    in
    let all = StringMap.fold
      (fun k v acc ->
	if cmp k
	then (k,v)::acc
	else acc)
      _kv []
    in
    Lwt.return all

  method get_cluster_cfgs () =
    failwith "get_cluster_cfgs not implemented in testbackend"

  method set_cluster_cfg cluster_id cfg =
    failwith "set_cluster not implemented in testbackend"

  method delete_prefix prefix = 
    let kv',n_deleted = 
      StringMap.fold 
        (fun k v ((s,c)as acc) -> 
          if String_extra.prefix_match prefix k 
          then (StringMap.remove k s,c+1)  
          else acc) _kv  (_kv,0) 
     in
    _kv <- kv';
    Lwt.return n_deleted

  method drop_master () =
    Lwt.return ()
end
