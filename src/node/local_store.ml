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

open Otc
open Hotc
open Mp_msg
open Lwt
open Update
open Log_extra
open Store

let _consensus_i db =
  try
    let i_string = Bdb.get db __i_key in
    let i,_ = Sn.sn_from i_string 0 in
    Lwt.return (Some i)
  with Not_found ->
    Lwt.return None


let _incr_i db =
  _consensus_i db >>= fun old_i ->
  let new_i =
    match old_i with
      | None -> Sn.start
      | Some i -> Sn.succ i
  in
  let new_is =
    let buf = Buffer.create 10 in
    let () = Sn.sn_to buf new_i in
    Buffer.contents buf
  in
  let () = Bdb.put db __i_key new_is in
  Lwt_log.debug_f "Local_store._incr_i old_i:%s -> new_i:%s" 
    (Log_extra.option_to_string Sn.string_of old_i) (Sn.string_of new_i)
  >>= fun () ->
  Lwt.return ()

let _p key = __prefix ^ key
let _f = function
  | Some x -> (Some (_p x))
  | None -> (Some __prefix)
let _l = function
  | Some x -> (Some (_p x))
  | None -> None

let _filter =
  Array.fold_left (fun acc key ->
    let l = String.length key in
    let key' = String.sub key 1 (l-1) in
    key'::acc) []

let _set bdb key value = Bdb.put bdb (_p key) value

let _delete bdb key    = Bdb.out bdb (_p key)

let _test_and_set bdb key expected wanted =
  let key' = _p key in
  try
    let g = Bdb.get bdb key' in
    match expected with
      | Some e when e = g ->
	begin
	  match wanted with 
	    | Some wanted_s ->
	      let () = Bdb.put bdb key' wanted_s in Some g
	    | None ->
	      let () = Bdb.out bdb key' in Some g
	end
      | _ -> Some g
  with Not_found ->
    match expected with
      | None -> 
	begin
	  match wanted with
	    | Some wanted_s ->
	      let () = Bdb.put bdb key' wanted_s in None
	    | None -> None		   
	end
      | Some v' -> None

let _set_master bdb master lease_start = 
  Bdb.put bdb __master_key master;
  let buffer =  Buffer.create 8 in
  let () = Llio.int64_to buffer lease_start in
  let lease = Buffer.contents buffer in
  Bdb.put bdb __lease_key lease

let rec _sequence bdb updates =
  let do_one = function
    | Update.Set (key,value) -> _set bdb key value
    | Update.Delete key -> _delete bdb key
    | Update.TestAndSet(key,expected, wanted) ->
      let _ = _test_and_set bdb key expected wanted in () (* TODO: do we want this? *)
    | Update.MasterSet (m,ls) -> _set_master bdb m ls
    | Update.Sequence us -> _sequence bdb us
    | Update.Nop -> ()
  in let get_key = function
    | Update.Set (key,value) -> Some key
    | Update.Delete key -> Some key
    | Update.TestAndSet (key, expected, wanted) -> Some key
    | _ -> None
  in let helper update =
    try
      do_one update
    with
      | Not_found ->
        let key = get_key update
        in match key with
        | Some key -> raise (Key_not_found key)
        | None -> raise Not_found
  in List.iter helper updates;

class local_store db =

object(self: #store)

  method exists key =
    Lwt.catch
      (fun () ->
	Hotc.transaction db (fun db -> Lwt.return (Bdb.get db (_p key))) >>= fun _ ->
	Lwt.return true
      )
      (function | Not_found -> Lwt.return false | exn -> Lwt.fail exn)

  method get key =
    Hotc.transaction db (fun db -> Lwt.return (Bdb.get db (_p key)))

  method multi_get keys = 
    Hotc.transaction db 
      (fun db -> 
	let vs = List.fold_left 
	  (fun acc key -> 
	    try
	      let v = Bdb.get db (_p key) in 
	      v::acc
	    with Not_found -> 
	      let exn = Common.XException(Arakoon_exc.E_NOT_FOUND,key) in 
	      raise exn
	  )
	  [] keys
	in 
	Lwt.return (List.rev vs))

  method range first finc last linc max =
    Hotc.transaction db
      (fun db -> Lwt.return (Bdb.range db (_f first) finc (_l last) linc max)) >>= fun r ->
    Lwt.return (_filter r)

  method range_entries first finc last linc max =
    Hotc.transaction db
      (fun db ->
	let keys_array = Bdb.range db (_f first) finc (_l last) linc max in
	let keys_list = Array.to_list keys_array in
	let x = List.fold_left
	  (fun ret_list k ->
	    let l = String.length k in
	    ((String.sub k 1 (l-1)), Bdb.get db k) :: ret_list )
          [] 
	  keys_list
	in Lwt.return x
      )

  method prefix_keys prefix max =
    Hotc.transaction db
      (fun db ->
	let keys_array = Bdb.prefix_keys db (_p prefix) max in
	let keys_list = _filter keys_array in
	Lwt.return keys_list
      )


  method set key value =
    Hotc.transaction db
      (fun db ->
	_incr_i db >>= fun () ->
	_set db key value;
	Lwt.return ())

  method set_master master lease=
    Hotc.transaction db
      (fun db ->
	_incr_i db >>= fun () ->
	_set_master db master lease;
	Lwt.return ()
      )

  method set_master_no_inc master lease = 
    Hotc.transaction db (fun db -> _set_master db master lease;Lwt.return ())

  method who_master () =
    Lwt.catch
      (fun () ->
	Hotc.transaction db
	  (fun db -> 
	    let m = Bdb.get db __master_key in
	    let ls_buff = Bdb.get db __lease_key in
	    let ls,_ = Llio.int64_from ls_buff 0 in
	    Lwt.return (Some (m,ls)))
      )
      (function | Not_found -> Lwt.return None | exn -> Lwt.fail exn)

  method delete key =
    Hotc.transaction db
      (fun db ->
	_incr_i db >>= fun () ->
	_delete db key;
	Lwt.return ())

  method test_and_set key expected wanted =
    Hotc.transaction db
      (fun db ->
	_incr_i db >>= fun () ->
	let r = _test_and_set db key expected wanted in
	Lwt.return r)

  method sequence updates =
    Hotc.transaction db
      (fun db ->
	_incr_i db >>= fun () ->
	_sequence db updates;
  	Lwt.return ())

  method consensus_i () =
    Hotc.transaction db (fun db -> _consensus_i db)

  method close () =
    Hotc.close db;
    Lwt_log.debug "local_store :: close () " >>= fun () ->
    Lwt.return ()

end

let make_local_store db_name =
  (* TODO: how can we ever close the database if we don't return it? *)
  Hotc.create db_name >>= fun db ->
  let store = new local_store db in
  let store2 = (store :> store) in
  Lwt.return store2
