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

open Unix.LargeFile


let _save_i i db =
  let is =
    let buf = Buffer.create 10 in
    let () = Sn.sn_to buf i in
    Buffer.contents buf
  in
  let () = Bdb.put db __i_key is in
  Lwt.return ()


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
	match _assert bdb k vo 
	with
	| true -> () 
	| false -> raise (Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED,k))
      end
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
  in List.iter helper updates




let _tx_with_incr (incr: unit -> Sn.t ) (db: Hotc.t) (f:Otc.Bdb.bdb -> 'a Lwt.t) =
  let new_i = incr () in 
  Lwt.catch
    (fun () ->
      Hotc.transaction db
    (fun db ->
    _save_i new_i db >>= fun () ->
      f db >>= fun (a:'a) ->
      Lwt.return a)
    )
    (fun ex ->
      Hotc.transaction db (_save_i new_i) >>= fun () ->
      Lwt.fail ex)

class local_store db_location db mlo store_i =

object(self: #store)
  val mutable _mlo = mlo
  val my_location = db_location
  val mutable _quiesced = false
  val mutable _store_i = store_i
  
  val _quiescedEx = Common.XException(Arakoon_exc.E_UNKNOWN_FAILURE,
     "Invalid operation on quiesced store")

  method quiesce () =
    begin
      if _quiesced then
        Lwt.fail(Failure "Store already quiesced. Blocking second attempt")
      else
        begin
          _quiesced <- true;
          self # reopen (fun () -> Lwt.return ()) >>= fun () ->
          Lwt.return ()
        end
    end
    
  method unquiesce () =
    _quiesced <- false; 
    self # reopen (fun () -> Lwt.return ()) 
  
  method quiesced () = _quiesced
  
  
  method exists key =
    Lwt.catch
      (fun () ->
        let bdb = Hotc.get_bdb db in
        Lwt.return (Bdb.get bdb (_p key)) >>= fun _ ->
        Lwt.return true
      )
      (function | Not_found -> Lwt.return false | exn -> Lwt.fail exn)

  method get key =
    Lwt.catch
      (fun () -> 
        let bdb = Hotc.get_bdb db in
        Lwt.return (Bdb.get bdb (_p key)))
      (function 
	| Failure _ -> Lwt.fail CorruptStore
	| exn -> Lwt.fail exn)

  method multi_get keys = 
    let bdb = Hotc.get_bdb db in
    let vs = List.fold_left 
	  (fun acc key -> 
	    try
	      let v = Bdb.get bdb (_p key) in 
	      v::acc
	    with Not_found -> 
	      let exn = Common.XException(Arakoon_exc.E_NOT_FOUND,key) in 
	      raise exn
	  )
	  [] keys
	  in 
	  Lwt.return (List.rev vs)
  
  method private _incr_i_cached () =
    let incr = 
    begin
      match _store_i with
        | None -> Sn.start
        | Some i -> (Sn.succ i)
    end; 
    in
    _store_i <- Some incr ; 
    incr
    
  method incr_i () =
    let new_i = self # _incr_i_cached () in
    begin
    if _quiesced 
    then
      Lwt.return ()
    else
      Hotc.transaction db (_save_i new_i)
    end
      
  method range first finc last linc max =
    let bdb = Hotc.get_bdb db in
    Lwt.return (Bdb.range bdb (_f first) finc (_l last) linc max) >>= fun r ->
    Lwt.return (_filter r)

  method range_entries first finc last linc max =
    let bdb = Hotc.get_bdb db in
	  let keys_array = Bdb.range bdb (_f first) finc (_l last) linc max in
	  let keys_list = Array.to_list keys_array in
	  let x = List.fold_left
	  (fun ret_list k ->
	    let l = String.length k in
	    ((String.sub k 1 (l-1)), Bdb.get bdb k) :: ret_list )
          [] 
	  keys_list
	  in Lwt.return x
      

  method prefix_keys prefix max =
    let bdb = Hotc.get_bdb db in
	  let keys_array = Bdb.prefix_keys bdb (_p prefix) max in
	  let keys_list = _filter keys_array in
	  Lwt.return keys_list
    

  method set key value =
    Lwt.catch
      (fun () -> _tx_with_incr (self # _incr_i_cached)  db (fun db -> _set db key value;Lwt.return ()))
      (function 
	| Failure _ -> Lwt.fail Server.FOOBAR
	| exn -> Lwt.fail exn)

  method set_master master lease =
    _tx_with_incr (self # _incr_i_cached)  db 
      (fun db ->
	_set_master db master lease ;
	_mlo <- Some (master,lease);
	Lwt.return ()
      )

  method set_master_no_inc master lease = 
    _mlo <- Some (master,lease);
    if _quiesced then
      Lwt.return ()
    else 
        Hotc.transaction db 
        (fun db -> _set_master db master lease;
               Lwt.return ()
        )

  method who_master () = Lwt.return _mlo


  method delete key =
    _tx_with_incr (self # _incr_i_cached) db
      (fun db -> _delete db key; Lwt.return ())

  method test_and_set key expected wanted =
    _tx_with_incr (self # _incr_i_cached) db
      (fun db -> 
	let r = _test_and_set db key expected wanted in
	Lwt.return r)

  method sequence updates =
    _tx_with_incr (self # _incr_i_cached) db
      (fun db -> _sequence db updates; Lwt.return ())

  method aSSert key (vo:string option) =
    _tx_with_incr (self # _incr_i_cached) db (fun db -> let r = _assert db key vo in Lwt.return r)

  method consensus_i () =
    Lwt.return _store_i

  method optimize () =
    Lwt_log.debug_f "optimizing store %s" (Hotc.filename db) >>= fun () ->
    Lwt_preemptive.detach
      (fun () -> let db = Hotc.get_bdb db in
		 Bdb.bdb_optimize db;
		 ()
      ) ()
    >>= fun () ->
    Lwt_log.debug "done optimizing store"


  method close () =
    Hotc.close db >>= fun () ->
    Lwt_log.debug "local_store :: close () " >>= fun () ->
    Lwt.return ()

  method get_filename () = Hotc.filename db

  method reopen f = 
    let mode = 
    begin
      if _quiesced then
        Bdb.readonly_mode
      else
        Bdb.default_mode
    end in 
    Lwt_log.debug "local_store::reopen calling Hotc::reopen" >>= fun () ->
    Hotc.reopen db f mode >>= fun () ->
    Lwt_log.debug "local_store::reopen Hotc::reopen succeeded" >>= fun () ->
    let bdb = Hotc.get_bdb db in
    _consensus_i bdb >>= fun store_i -> 
    _store_i <- store_i ;
    Lwt.return ()

    
  method get_key_count () =
    Lwt_log.debug "local_store::get_key_count" >>= fun () ->
    Hotc.transaction db (fun db -> Lwt.return ( Bdb.get_key_count db ) ) >>= fun raw_count ->
    (* Leave out administrative keys *)
    Lwt.return ( Int64.sub raw_count 3L ) 

  method copy_store (oc: Lwt_io.output_channel) =
    if _quiesced then
    begin
      let db_name = my_location in
      let stat = Unix.LargeFile.stat db_name in
      let length = stat.st_size in
      Lwt_log.debug_f "store:: copy_store (filesize is %Li bytes)" length >>= fun ()->
      Llio.output_int oc 0 >>= fun () ->
      Llio.output_int64 oc length >>= fun () ->
      Lwt_io.with_file
        ~flags:[Unix.O_RDONLY]
        ~mode:Lwt_io.input
        db_name (fun ic -> Llio.copy_stream ~length ~ic ~oc)
      >>= fun () ->
      Lwt_io.flush oc
    end 
    else
    begin
      let ex = Common.XException(Arakoon_exc.E_UNKNOWN_FAILURE, "Can only copy a quiesced store" ) in
      raise ex
    end

end

let make_local_store db_name =
  Hotc.create db_name >>= fun db ->
  Hotc.transaction db _who_master >>= fun mlo ->
  Hotc.transaction db _consensus_i >>= fun store_i ->
  let store = new local_store db_name db mlo store_i in
  let store2 = (store :> store) in
  Lwt.return store2
