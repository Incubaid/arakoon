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
open Range
open Routing
open Log_extra
open Store



let _consensus_i db =
  try
    let i_string = Bdb.get db __i_key in
    let i,_ = Sn.sn_from i_string 0 in
    Lwt.return (Some i)
  with Not_found ->
    Lwt.return None

let _get_range db = 
  try
    let range_s = Bdb.get db __range_key in
    let range,_ = Range.range_from range_s 0 in
    Lwt.return range
  with Not_found -> Lwt.return Range.max

let _get_routing db = 
  try 
    let routing_s = Bdb.get db __routing_key in
    let routing,_ = Routing.routing_from routing_s 0 in
    Lwt.return (Some routing)
  with Not_found -> Lwt.return None

let _set_range db range = 
  let buf = Buffer.create 80 in
  let () = Range.range_to buf range in
  let range_s = Buffer.contents buf in
  Bdb.put db __range_key range_s

let _set_routing db routing =
  let buf = Buffer.create 80 in
  let () = Routing.routing_to buf routing in
  let routing_s = Buffer.contents buf in
  Bdb.put db __routing_key routing_s

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
  (* Lwt_log.debug_f "Local_store._incr_i old_i:%s -> new_i:%s" 
    (Log_extra.option_to_string Sn.string_of old_i) (Sn.string_of new_i)
  >>= fun () -> *)
  Lwt.return ()

let _who_master db = 
  try
    let m = Bdb.get db __master_key in
    let ls_buff = Bdb.get db __lease_key in
    let ls,_ = Llio.int64_from ls_buff 0 in
    Lwt.return (Some (m,ls))
  with Not_found -> 
    Lwt.return None

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

let _get bdb key = Bdb.get bdb (_p key)

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

let _range_entries bdb first finc last linc max =
  let keys_array = Bdb.range bdb (_f first) finc (_l last) linc max in
  let keys_list = Array.to_list keys_array in
  let x = List.fold_left
    (fun ret_list k ->
      let l = String.length k in
      ((String.sub k 1 (l-1)), Bdb.get bdb k) :: ret_list )
    [] 
    keys_list
  in x




let _assert bdb key vo =
  let pk = _p key in
  match vo with
    | None ->
      begin
	try let _ = Bdb.get bdb pk in false
	with Not_found -> true
      end
    | Some v ->
      begin
	try let v' = Bdb.get bdb pk in v = v'
	with Not_found -> false
      end



open Registry

class bdb_user_db bdb = 
object (self : # user_db)

  method set k v = _set bdb k v
    
  method get k = _get bdb k

  method delete k = _delete bdb k
  
  method test_and_set k e w = _test_and_set bdb k e w

  method range_entries first finc last linc max =
    _range_entries bdb first finc last linc max
end


let _user_function bdb (name:string) (po:string option) = 
  let f = Registry.lookup name in
  let bdb_inner = new bdb_user_db bdb in
  let inner = (bdb_inner :> user_db) in
  let ro = f inner po in
  ro


let _set_master bdb master (lease_start:int64) =  
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
    | Update.Assert(k,vo) ->
      begin
	match _assert bdb k vo with
	  | true -> ()
	  | false -> 
	    raise (Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED,k))
      end
    | Update.UserFunction(name,po) ->
      let _ = _user_function bdb name po in ()
    | Update.MasterSet (m,ls) -> _set_master bdb m ls
    | Update.Sequence us -> _sequence bdb us
    | Update.SetRange range -> _set_range bdb range
    | Update.SetRouting r   -> _set_routing bdb r
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
  in List.iter helper updates


let _set_master bdb master (lease_start:int64) =  
  Bdb.put bdb __master_key master;
  let buffer =  Buffer.create 8 in
  let () = Llio.int64_to buffer lease_start in
  let lease = Buffer.contents buffer in
  Bdb.put bdb __lease_key lease


let _tx_with_incr db (f:Otc.Bdb.bdb -> 'a Lwt.t) = 
  Lwt.catch
    (fun () ->
      Hotc.transaction db
	(fun db ->
	  _incr_i db >>= fun () ->
	  f db >>= fun (a:'a) ->
	  Lwt.return a)
    )
    (fun ex ->
      Hotc.transaction db _incr_i >>= fun () ->
      Lwt.fail ex)

class local_store db (range:Range.t) (routing:Routing.t option) mlo =

object(self: #store)
  val mutable _range = range
  val mutable _routing = routing
  val mutable _mlo = mlo

  method _range_ok key =
    let ok = Range.is_ok _range key in
    if ok 
    then ()
    else 
      let ex = Common.XException(Arakoon_exc.E_UNKNOWN_FAILURE,
				 Printf.sprintf "%s not in range" key)
      in raise ex
      
  method exists key =
    Lwt.catch
      (fun () ->
	Hotc.transaction db (fun db -> Lwt.return (Bdb.get db (_p key))) >>= fun _ ->
	Lwt.return true
      )
      (function | Not_found -> Lwt.return false | exn -> Lwt.fail exn)

  method get key =
    Lwt.catch
      (fun () -> Hotc.transaction db (fun db -> Lwt.return (Bdb.get db (_p key))))
      (function 
	| Failure _ -> Lwt.fail CorruptStore
	| exn -> Lwt.fail exn)

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
  
  method incr_i () = Hotc.transaction db _incr_i

      
  method range first finc last linc max =
    Hotc.transaction db
      (fun db -> Lwt.return (Bdb.range db (_f first) finc (_l last) linc max)) >>= fun r ->
    Lwt.return (_filter r)

  method range_entries first finc last linc max =
    Hotc.transaction db
      (fun db -> let r = _range_entries db first finc last linc max in
		 Lwt.return r
      )
     
  method prefix_keys prefix max =
    Hotc.transaction db
      (fun db ->
	let keys_array = Bdb.prefix_keys db (_p prefix) max in
	let keys_list = _filter keys_array in
	Lwt.return keys_list
      )


  method set key value =
    Lwt.catch
      (fun () -> _tx_with_incr db (fun db -> _set db key value; Lwt.return ()))
      (function 
	| Failure _ -> Lwt.fail Server.FOOBAR
	| exn -> Lwt.fail exn)

  method set_master master lease =
    _tx_with_incr db
      (fun db ->
	_set_master db master lease ;
	_mlo <- Some (master,lease);
	Lwt.return ()
      )

  method set_master_no_inc master lease = 
    Hotc.transaction db 
      (fun db -> _set_master db master lease;
	_mlo <- Some (master,lease);
	Lwt.return ()
      )

  method who_master () = Lwt.return _mlo

  method delete key =
    _tx_with_incr db (fun db -> _delete db key; Lwt.return ())

  method test_and_set key expected wanted =
    _tx_with_incr db 
      (fun db ->
	let r = _test_and_set db key expected wanted in
	Lwt.return r)

  method sequence updates =
    _tx_with_incr db
      (fun db -> _sequence db updates; Lwt.return ())

  method aSSert key (vo:string option) =
    _tx_with_incr db (fun db -> let r = _assert db key vo in Lwt.return r)

  method user_function name (po:string option) = 
    Lwt_log.debug_f "user_function :%s" name >>= fun () ->
    _tx_with_incr db
      (fun db -> 
	let (ro:string option) = _user_function db name po in
	Lwt.return ro)
      
  method consensus_i () =
    Hotc.transaction db (fun db -> _consensus_i db)
      
  method close () =
    Hotc.close db >>= fun () ->
    Lwt_log.debug "local_store :: close () " >>= fun () ->
    Lwt.return ()
      
  method get_filename () = Hotc.filename db
    
  method reopen f = 
    Lwt_log.debug "local_store :: reopen() " >>= fun () ->
    Hotc.reopen db f >>= fun () ->
    Lwt.return ()

  method set_range range = 
    Lwt_log.debug_f "set_range %s" (Range.to_string range) >>= fun () ->
    _range <- range;
    Hotc.transaction db (fun db -> _set_range db range;Lwt.return ())

  method get_routing () = 
    Lwt_log.debug "get_routing " >>= fun () ->
    match _routing with
      | None -> Llio.lwt_failfmt "no routing"
      | Some r -> Lwt.return r

  method set_routing r =
    Lwt_log.debug_f "set_routing %s" (Routing.to_s r) >>= fun () ->
    _routing <- Some r;
    Hotc.transaction db (fun db -> _set_routing db r; Lwt.return ())
end

let make_local_store db_name =
  Hotc.create db_name >>= fun db ->
  Hotc.transaction db _get_range >>= fun range ->
  Hotc.transaction db _get_routing >>= fun routing_o ->
  Hotc.transaction db _who_master >>= fun mlo ->
  let store = new local_store db range routing_o mlo in
  let store2 = (store :> store) in
  Lwt.return store2
