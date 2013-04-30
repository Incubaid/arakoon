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

open Mp_msg
open Lwt
open Update
open Interval
open Routing
open Log_extra
open Store
open Unix.LargeFile


module B = Camltc.Bdb

let _save_i i db =
  let is =
    let buf = Buffer.create 10 in
    let () = Sn.sn_to buf i in
    Buffer.contents buf
  in
  let () = B.put db __i_key is in
  Lwt.return ()

let _consensus_i db =
  try
    let i_string = B.get db __i_key in
    let i,_ = Sn.sn_from i_string 0 in
    Lwt.return (Some i)
  with Not_found ->
    Lwt.return None

let _get_interval db =
  try
    let interval_s = B.get db __interval_key in
    let interval,_ = Interval.interval_from interval_s 0 in
    Lwt.return interval
  with Not_found -> Lwt.return Interval.max

let _set_interval db range =
  let buf = Buffer.create 80 in
  let () = Interval.interval_to buf range in
  let range_s = Buffer.contents buf in
  B.put db __interval_key range_s

let _set_routing db routing =
  let buf = Buffer.create 80 in
  let () = Routing.routing_to buf routing in
  let routing_s = Buffer.contents buf in
  B.put db __routing_key routing_s

let _get_routing_non_lwt db =
  try
    let routing_s = B.get db __routing_key in
    let routing,_ = Routing.routing_from routing_s 0 in
    Some routing
  with Not_found -> None

let _get_routing db =
    Lwt.return (_get_routing_non_lwt db)

let _set_routing_delta db left sep right =
  let m_r = _get_routing_non_lwt db in
  let new_r =
    begin
      match m_r with
        | None ->
          begin
            Routing.build ([(left, sep)], right)
          end
        | Some r ->
          begin
            Routing.change r left sep right
          end
      end
  in
  let new_r' = Routing.compact new_r in
  _set_routing db new_r';
  new_r'

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
  let () = B.put db __i_key new_is in
  (* Lwt_log.debug_f "Local_store._incr_i old_i:%s -> new_i:%s"
    (Log_extra.option_to_string Sn.string_of old_i) (Sn.string_of new_i)
  >>= fun () -> *)
  Lwt.return ()

let _who_master db =
  try
    let m = B.get db __master_key in
    let ls_buff = B.get db __lease_key in
    let ls,_ = Llio.int64_from ls_buff 0 in
    Lwt.return (Some (m,ls))
  with Not_found ->
    Lwt.return None


let _get _pf bdb key = B.get bdb (_pf ^ key)

let _set _pf bdb key value = B.put bdb (_pf ^ key) value

let _delete _pf bdb key    = B.out bdb (_pf ^ key)

let _delete_prefix _pf bdb prefix = 
  let xprefix = _pf ^ prefix in
  B.delete_prefix bdb xprefix

let _test_and_set _pf bdb key expected wanted =
  let key' = _pf ^ key in
  try
    let g = B.get bdb key' in
    match expected with
      | Some e when e = g ->
	begin
	  match wanted with
	    | Some wanted_s ->
	      let () = B.put bdb key' wanted_s in Some g
	    | None ->
	      let () = B.out bdb key' in Some g
	end
      | _ -> Some g
  with Not_found ->
    match expected with
      | None ->
	begin
	  match wanted with
	    | Some wanted_s ->
	      let () = B.put bdb key' wanted_s in None
	    | None -> None
	end
      | Some v' -> None

let _range_entries _pf bdb first finc last linc max =
  let keys_array = B.range bdb (_f _pf first) finc (_l _pf last) linc max in
  let keys_list = Array.to_list keys_array in
  let pl = String.length _pf in
  let x = List.fold_left
    (fun ret_list k ->
      let l = String.length k in
      ((String.sub k pl (l-pl)), B.get bdb k) :: ret_list )
    []
    keys_list
  in x

let _assert _pf bdb key vo =
  let pk = _pf ^ key in
  match vo with
    | None ->
      begin
	try let _ = B.get bdb pk in false
	with Not_found -> true
      end
    | Some v ->
      begin
	try let v' = B.get bdb pk in v = v'
	with Not_found -> false
      end

let _assert_exists _pf bdb key =
  let pk = _pf ^ key in
	try let _ = B.get bdb pk in true
	with Not_found -> false


let copy_store old_location new_location overwrite =
  File_system.exists old_location >>= fun src_exists ->
  if not src_exists
  then
    Lwt_log.debug_f "File at %s does not exist" old_location >>= fun () ->
    raise Not_found
  else
  begin
    File_system.exists new_location >>= fun dest_exists ->
    begin
      if dest_exists && overwrite
      then
        Lwt_unix.unlink new_location
      else
        Lwt.return ()
    end >>= fun () ->
    begin
      if dest_exists && not overwrite
      then
        Lwt_log.debug_f "Not relocating store from %s to %s, destination exists" old_location new_location
      else
        File_system.copy_file old_location new_location
    end
  end
open Registry

class bdb_user_db interval bdb =
  let test k =
    if not (Interval.is_ok interval k) then
      raise (Common.XException (Arakoon_exc.E_OUTSIDE_INTERVAL, k))
  in
  let test_option = function
    | None -> ()
    | Some k -> test k
  in
  let test_range first last = test_option first; test_option last
  in

object (self : # user_db)

  method set k v = test k ; _set __prefix bdb k v
  method get k   = test k ; _get __prefix bdb k

  method delete k = test k ; _delete __prefix bdb k

  method test_and_set k e w = test k; _test_and_set __prefix bdb k e w

  method range_entries first finc last linc max =
    test_range first last;
    _range_entries __prefix bdb first finc last linc max
end


let _user_function bdb (interval:Interval.t) (name:string) (po:string option) =
  let f = Registry.lookup name in
  let bdb_inner = new bdb_user_db interval bdb in
  let inner = (bdb_inner :> user_db) in
  let ro = f inner po in
  ro


let _set_master bdb master (lease_start:int64) =
  B.put bdb __master_key master;
  let buffer =  Buffer.create 8 in
  let () = Llio.int64_to buffer lease_start in
  let lease = Buffer.contents buffer in
  B.put bdb __lease_key lease

let rec _sequence _pf bdb interval updates =
  let do_one = function
    | Update.Set (key,value) -> _set _pf bdb key value
    | Update.Delete key -> _delete _pf bdb key
    | Update.TestAndSet(key,expected, wanted) ->
      let _ = _test_and_set _pf bdb key expected wanted in () (* TODO: do we want this? *)
    | Update.Assert(k,vo) ->
      begin
	    match _assert _pf bdb k vo with
	      | true -> ()
	      | false ->
	        raise (Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED,k))
      end
    | Update.Assert_exists(k) ->
      begin
	    match _assert_exists _pf bdb k with
	      | true -> ()
	      | false ->
	        raise (Arakoon_exc.Exception(Arakoon_exc.E_ASSERTION_FAILED,k))
      end
    | Update.UserFunction(name,po) ->
      let _ = _user_function bdb interval name po in ()
    | Update.MasterSet (m,ls) -> _set_master bdb m ls
    | Update.Sequence us 
    | Update.SyncedSequence us -> _sequence _pf bdb interval us
    | Update.SetInterval interval -> _set_interval bdb interval
    | Update.SetRouting r   -> _set_routing bdb r
    | Update.SetRoutingDelta (l, s, r) -> let _ = _set_routing_delta bdb l s r in ()
    | Update.AdminSet(k,vo) ->
    begin
      match _pf with
        | pf when pf = __adminprefix ->
          begin
          match vo with
            | None -> _delete _pf bdb k
            | Some v -> _set _pf bdb k v
          end
        | _ -> raise  (Arakoon_exc.Exception(Arakoon_exc.E_UNKNOWN_FAILURE, "Cannot modify admin keys in user sequence"))
    end
    | Update.Nop -> ()
    | Update.DeletePrefix prefix -> let _ = _delete_prefix _pf bdb prefix in ()
  in 
  let get_key = function
    | Update.Set (key,value) -> Some key
    | Update.Delete key -> Some key
    | Update.TestAndSet (key, expected, wanted) -> Some key
    | _ -> None
  in 
  let helper update =
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
  B.put bdb __master_key master;
  let buffer =  Buffer.create 8 in
  let () = Llio.int64_to buffer lease_start in
  let lease = Buffer.contents buffer in
  B.put bdb __lease_key lease

let _get_j bdb =
  try
    let j_string = B.get bdb __j_key in
    int_of_string j_string
  with _ -> 0

let _set_j bdb j =
  B.put bdb __j_key (string_of_int j)


let get_construct_params db_name ~mode=
  Camltc.Hotc.create db_name ~mode >>= fun db ->
  Camltc.Hotc.read db _get_interval >>= fun interval ->
  Camltc.Hotc.read db _get_routing >>= fun routing_o ->
  Camltc.Hotc.read db _who_master >>= fun mlo ->
  Camltc.Hotc.read db _consensus_i >>= fun store_i ->
  Lwt.return (db, interval, routing_o, mlo, store_i)

class local_store (db_location:string) (db:Camltc.Hotc.t)
  (interval:Interval.t) (routing:Routing.t option) mlo store_i =

object(self: #store)
  val mutable my_location = db_location
  val mutable _interval = interval
  val mutable _routing = routing
  val mutable _mlo = mlo
  val mutable _quiesced = false
  val mutable _store_i = store_i
  val mutable _closed = false (* set when close method is called *)
  val mutable _tx = None
  val mutable _tx_lock = None
  val _tx_lock_mutex = Lwt_mutex.create ()

  val _quiescedEx = Common.XException(Arakoon_exc.E_UNKNOWN_FAILURE,
       "Invalid operation on quiesced store")

  method private _wrap_exception name (f:unit -> 'a Lwt.t) = 
    (* this should not be a method as it hinders polymorphism *)
    Lwt.catch
      f 
      (function
        | Failure s -> 
          begin
            Lwt_log.debug_f "%s Failure %s: => FOOBAR? %b" name s _closed >>= fun () ->
            if _closed 
            then Lwt.fail (Common.XException (Arakoon_exc.E_GOING_DOWN, s ^ " : database closed"))
            else Lwt.fail Server.FOOBAR
          end
        | exn -> Lwt.fail exn)    

  method private _with_tx : 'a. transaction -> (Camltc.Hotc.bdb -> 'a Lwt.t) -> (int -> int) -> 'a Lwt.t =
    fun tx f new_j ->
      match _tx with
        | None -> failwith "not in a transaction"
        | Some (tx', db) ->
            if tx != tx'
            then
              failwith "the provided transaction is not the current transaction of the store"
            else
              let j = _get_j db in
              f db >>= fun a ->
              let () = _set_j db (new_j j) in
              Lwt.return a

  method private _update_in_tx : 'a. transaction -> (Camltc.Hotc.bdb -> 'a Lwt.t) -> 'a Lwt.t =
    fun tx f ->
      self # _with_tx tx f ((+) 1)

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
    let current_i = _store_i in
    Lwt.catch
      (fun () ->
        Lwt.finalize
          (fun () ->
            Camltc.Hotc.transaction db
              (fun db ->
                let tx = new transaction in
                _tx <- Some (tx, db);

                let t0 = Unix.gettimeofday() in
                f tx >>= fun a ->
                let t = ( Unix.gettimeofday() -. t0) in
                if t > 1.0
                then begin
                  Lwt_log.info_f "Tokyo cabinet transaction took %fs" t >>= fun () ->
                  Lwt.return a
                end
                else
                  Lwt.return a))
          (fun () -> _tx <- None; Lwt.return ()))
      (fun exn ->
        (* restore i when transaction failed *)
        _store_i <- current_i;
        Lwt.fail exn)

  method quiesce () =
    begin
      if _quiesced 
      then Lwt.fail(Failure "Store already quiesced. Blocking second attempt")
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

  method optimize () = 
    let db_optimal = my_location ^ ".opt" in
    Lwt_log.debug "Copying over db file" >>= fun () ->
    Lwt_io.with_file
        ~flags:[Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC]
        ~mode:Lwt_io.output
        db_optimal (fun oc -> self # copy_store ~_networkClient:false oc )
    >>= fun () ->
    begin 
      Lwt_log.debug_f "Creating new db object at location %s" db_optimal >>= fun () ->
      Camltc.Hotc.create db_optimal >>= fun db_opt ->
      Lwt.finalize
      ( fun () ->
        Lwt_log.info "Optimizing db copy" >>= fun () ->
        Camltc.Hotc.optimize db_opt >>= fun () ->
        Lwt_log.info "Optimize db copy complete"
      ) 
      ( fun () ->
        Camltc.Hotc.close db_opt
      )
    end >>= fun () ->
    File_system.rename db_optimal my_location >>= fun () ->
    self # reopen (fun () -> Lwt.return ())

  method defrag() = 
    Lwt_log.debug "local_store :: defrag" >>= fun () ->
    Camltc.Hotc.defrag db >>= fun rc ->
    Lwt_log.debug_f "local_store %s :: defrag done: rc=%i" db_location rc>>= fun () ->
    Lwt.return ()

  (*method private open_db () =
    Hotc._open_lwt db *)

  method _interval_ok key =
    let ok = Interval.is_ok _interval key in
    if ok
    then ()
    else
      let ex = Common.XException(Arakoon_exc.E_UNKNOWN_FAILURE,
				                 Printf.sprintf "%s not in interval" key)
      in raise ex

  method get_j () =
    let j =
      try
        let bdb = Camltc.Hotc.get_bdb db in
        let j_string = B.get bdb __j_key in
        int_of_string j_string
      with _ -> 0 in
    Lwt.return j

  method exists ?(_pf = __prefix) key =
    let bdb = Camltc.Hotc.get_bdb db in
    let r = B.exists bdb (_pf ^ key) in
    Lwt.return r
    
  method get ?(_pf = __prefix) key =
    Lwt.catch
      (fun () ->
        let bdb = Camltc.Hotc.get_bdb db in
        Lwt.return (B.get bdb (_pf ^ key)))
      (function
	| Failure msg -> 
        Lwt_log.debug_f "local_store: Failure %Swhile GET (_closed:%b)" msg _closed >>= fun () ->
        if _closed 
        then
          Lwt.fail (Common.XException (Arakoon_exc.E_GOING_DOWN, 
                                       Printf.sprintf "GET %S database already closed" key))
        else
          Lwt.fail CorruptStore
	| exn -> Lwt.fail exn)

  method multi_get ?(_pf = __prefix) keys =
    let bdb = Camltc.Hotc.get_bdb db in
    let vs = List.fold_left
	  (fun acc key ->
	    try
	      let v = B.get bdb (_pf ^ key) in
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

  method incr_i tx =
    let new_i = self # _incr_i_cached () in
    begin
      if _quiesced 
      then Lwt.return ()
      else self # _with_tx tx (_save_i new_i) (fun _ -> 0)
    end


  method range ?(_pf=__prefix) first finc last linc max =
    let bdb = Camltc.Hotc.get_bdb db in
    Lwt.return (B.range bdb (_f _pf first) finc (_l _pf last) linc max) >>= fun r ->
    Lwt.return (_filter _pf r)

  method range_entries ?(_pf=__prefix) first finc last linc max =
    let bdb = Camltc.Hotc.get_bdb db in
    let r = _range_entries _pf bdb first finc last linc max in
    Lwt.return r

  method rev_range_entries ?(_pf=__prefix) first finc last linc max =
    let bdb = Camltc.Hotc.get_bdb db in
    let r = B.rev_range_entries _pf bdb first finc last linc max in
    Lwt.return r

  method prefix_keys ?(_pf=__prefix) prefix max =
    let bdb = Camltc.Hotc.get_bdb db in
    let keys_array = B.prefix_keys bdb (_pf ^ prefix) max in
	let keys_list = _filter _pf keys_array in
	Lwt.return keys_list

  method set tx ?(_pf=__prefix) key value =
    self # _wrap_exception "set"
      (fun () -> self # _update_in_tx tx
        (fun db -> _set _pf db key value; Lwt.return ()))

  method delete tx ?(_pf=__prefix) key =
    self # _wrap_exception "delete"
      (fun () -> self # _update_in_tx tx
        (fun db -> _delete _pf db key; Lwt.return ()) )

  method test_and_set tx ?(_pf=__prefix) key expected wanted =
    self # _update_in_tx tx
      (fun db ->
	    let r = _test_and_set _pf db key expected wanted in
	    Lwt.return r)
      
  method set_master tx master lease =
    self # _update_in_tx tx
      (fun db ->
	    _set_master db master lease ;
	    _mlo <- Some (master,lease);
	    Lwt.return ()
      )

  method set_master_no_inc master lease =
    _mlo <- Some (master,lease);
    if _quiesced 
    then Lwt.return ()
    else
      Camltc.Hotc.transaction db
        (fun db -> _set_master db master lease;
	      Lwt.return ()
        )

  method who_master () = _mlo


  method delete_prefix tx ?(_pf=__prefix) prefix = 
    Lwt_log.debug_f "local_store :: delete_prefix %S" prefix >>= fun () ->
    self # _update_in_tx tx
      (fun db -> 
        let c = _delete_prefix _pf db prefix in 
        Lwt.return c)

  method sequence tx ?(_pf=__prefix) updates =
    self # _update_in_tx tx
      (fun db -> _sequence _pf db _interval updates; Lwt.return ())

  method aSSert tx ?(_pf=__prefix) key (vo:string option) =
    self # _update_in_tx tx
      (fun db -> let r = _assert _pf db key vo in Lwt.return r)

  method aSSert_exists tx ?(_pf=__prefix) key =
    self # _update_in_tx tx
      (fun db -> let r = _assert_exists _pf db key in Lwt.return r)

  method user_function tx name (po:string option) =
    Lwt_log.debug_f "user_function :%s" name >>= fun () ->
    self # _update_in_tx tx
      (fun db ->
	    let (ro:string option) = _user_function db _interval name po in
	    Lwt.return ro)

  method consensus_i () = _store_i

  method close () =
    _closed <- true;
    Camltc.Hotc.close db >>= fun () ->  
    Lwt_log.info_f "local_store %S :: closed  () " db_location >>= fun () ->
    Lwt.return ()

  method get_location () = Camltc.Hotc.filename db

  method reopen f =
    let mode =
    begin
      if _quiesced then
        B.readonly_mode
      else
        B.default_mode
    end in
    Lwt_log.debug_f "local_store %S::reopen calling Hotc::reopen" db_location >>= fun () ->
    Camltc.Hotc.reopen db f mode >>= fun () ->
    Lwt_log.debug "local_store::reopen Hotc::reopen succeeded" >>= fun () ->
    (* Hotc.transaction db _consensus_i >>= fun store_i -> *)
    let bdb = Camltc.Hotc.get_bdb db in
    _consensus_i bdb >>= fun store_i ->
    _store_i <- store_i ;
    Lwt.return ()

  method set_interval tx interval =
    Lwt_log.debug_f "set_interval %s" (Interval.to_string interval)
    >>= fun () ->
    self # _update_in_tx tx
      (fun db ->
	    _set_interval db interval;
	    _interval <- interval;
	    Lwt.return ()
      )

  method get_interval () = Lwt.return _interval

  method get_routing () =
    Lwt_log.debug "get_routing " >>= fun () ->
    match _routing with
      | None -> Lwt.fail Not_found
      | Some r -> Lwt.return r

  method set_routing tx r =
    Lwt_log.debug_f "set_routing %s" (Routing.to_s r) >>= fun () ->
    _routing <- Some r;
    self # _update_in_tx tx (fun db -> _set_routing db r; Lwt.return ())

  method set_routing_delta tx left sep right =
    Lwt_log.debug "local_store::set_routing_delta" >>= fun () ->
    self # _update_in_tx tx
      (fun db ->
        let r = _set_routing_delta db left sep right in
        _routing <- Some r ;
        Lwt_log.debug_f "set_routing to %s" (Routing.to_s r) >>= fun () ->
        Lwt.return ()
      )

  method get_key_count ?(_pf=__prefix) () =
    Lwt_log.debug "local_store::get_key_count" >>= fun () ->
    Camltc.Hotc.transaction db (fun db -> Lwt.return ( B.get_key_count db ) ) >>= fun raw_count ->
    (* Leave out administrative keys *)
    self # prefix_keys ~_pf:__adminprefix "" (-1) >>= fun admin_keys ->
    let admin_key_count = List.length admin_keys in
    Lwt.return ( Int64.sub raw_count (Int64.of_int admin_key_count) )

  method copy_store ?(_networkClient=true) (oc: Lwt_io.output_channel) =
    if _quiesced 
    then
      begin
        let db_name = my_location in
        let stat = Unix.LargeFile.stat db_name in
        let length = stat.st_size in
        Lwt_log.debug_f "store:: copy_store (filesize is %Li bytes)" length >>= fun ()->
        begin
          if _networkClient 
          then
            Llio.output_int oc 0 >>= fun () ->
          Llio.output_int64 oc length 
          else Lwt.return ()
        end
        >>= fun () ->
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

  method relocate new_location =
    copy_store my_location new_location true >>= fun () ->
    let old_location = my_location in
    let () = my_location <- new_location in
    Lwt_log.debug_f "Attempting to unlink file '%s'" old_location >>= fun () ->
    File_system.unlink old_location >>= fun () ->
    Lwt_log.debug_f "Successfully unlinked file at '%s'" old_location


  method get_fringe border direction =
    let cursor_init, get_next, key_cmp =
      begin
        match direction with
        | Routing.UPPER_BOUND ->
          let skip_keys lcdb cursor =
            let () = B.first lcdb cursor in
            let rec skip_admin_key () =
              begin
                let k = B.key lcdb cursor in
                if k.[0] <> __adminprefix.[0]
                then
                  Lwt.ignore_result ( Lwt_log.debug_f "Not skipping key: %s" k )
                else
                  begin
                    Lwt.ignore_result ( Lwt_log.debug_f "Skipping key: %s" k );
                    begin
                      try
                        B.next lcdb cursor;
                        skip_admin_key ()
                      with Not_found -> ()
                    end
                  end
              end
            in
            skip_admin_key ()
          in
          let cmp =
            begin
              match border with
                | Some b ->  (fun k -> k >= (__prefix ^ b))
                | None -> (fun k -> false)
            end
          in
          skip_keys, B.next, cmp
        | Routing.LOWER_BOUND ->
          let cmp =
            begin
              match border with
                | Some b -> (fun k -> k < (__prefix ^ b) or k.[0] <> __prefix.[0])
                | None -> (fun k -> k.[0] <> __prefix.[0])
            end
          in
          B.last, B.prev, cmp
      end
    in
    Lwt_log.debug_f "local_store::get_fringe %S" (Log_extra.string_option2s border) >>= fun () ->
    let buf = Buffer.create 128 in
    Lwt.finalize
      (fun () ->
        Camltc.Hotc.transaction db
          (fun txdb ->
            Camltc.Hotc.with_cursor txdb
            (fun lcdb cursor ->
              Buffer.add_string buf "1\n";
              let limit = 1024 * 1024 in
              let () = cursor_init lcdb cursor in
              Buffer.add_string buf "2\n";
              let r =
                let rec loop acc ts =
                  begin
                    try
                      let k = B.key   lcdb cursor in
                      let v = B.value lcdb cursor in
                      if ts >= limit  or (key_cmp k)
                      then acc
                      else
                        let pk = String.sub k 1 (String.length k -1) in
                        let acc' = (pk,v) :: acc in
                        Buffer.add_string buf (Printf.sprintf "pk=%s v=%s\n" pk v);
                        let ts' = ts + String.length k + String.length v in
                        begin
                          try
                            get_next lcdb cursor ;
                            loop acc' ts'
                          with Not_found ->
                            acc'
                        end
                    with Not_found ->
                      acc
                  end

              in
              loop [] 0
              in
              Lwt.return r
            )
          )
      )
      (fun () ->
        Lwt_log.debug_f "buf:%s" (Buffer.contents buf)
    )
end

let make_local_store ?(read_only=false) db_name =
  let mode =
    if read_only
    then B.readonly_mode
    else B.default_mode
  in
  Lwt_log.debug_f "Creating local store at %s" db_name >>= fun () ->
  get_construct_params db_name ~mode
  >>= fun (db, interval, routing_o, mlo, store_i) ->
  let store = new local_store db_name db interval routing_o mlo store_i in
  let store2 = (store :> store) in
  Lwt.return store2

