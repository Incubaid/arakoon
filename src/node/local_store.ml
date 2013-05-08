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

let _who_master db =
  try
    let m = B.get db __master_key in
    let ls_buff = B.get db __lease_key in
    let ls,_ = Llio.int64_from ls_buff 0 in
    Lwt.return (Some (m,ls))
  with Not_found ->
    Lwt.return None


let _get bdb key = B.get bdb key

let _set bdb key value = B.put bdb key value

let _delete bdb key    = B.out bdb key

let _delete_prefix bdb prefix =
  B.delete_prefix bdb prefix

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

let _set_master bdb master (lease_start:int64) =
  B.put bdb __master_key master;
  let buffer =  Buffer.create 8 in
  let () = Llio.int64_to buffer lease_start in
  let lease = Buffer.contents buffer in
  B.put bdb __lease_key lease


let _set_master bdb master (lease_start:int64) =
  B.put bdb __master_key master;
  let buffer =  Buffer.create 8 in
  let () = Llio.int64_to buffer lease_start in
  let lease = Buffer.contents buffer in
  B.put bdb __lease_key lease


let get_construct_params db_name ~mode=
  Camltc.Hotc.create db_name ~mode >>= fun db ->
  Camltc.Hotc.read db _get_interval >>= fun interval ->
  Camltc.Hotc.read db _get_routing >>= fun routing_o ->
  Camltc.Hotc.read db _who_master >>= fun mlo ->
  Lwt.return (db, interval, routing_o, mlo)

class local_store (db_location:string) (db:Camltc.Hotc.t)
  (interval:Interval.t) (routing:Routing.t option) mlo =

object(self: #simple_store)
  val mutable my_location = db_location
  val mutable _interval = interval
  val mutable _routing = routing
  val mutable _mlo = mlo
  val mutable _quiesced = false
  val mutable _closed = false (* set when close method is called *)
  val mutable _tx = None
  val mutable _tx_lock = None
  val _tx_lock_mutex = Lwt_mutex.create ()

  val _quiescedEx = Common.XException(Arakoon_exc.E_UNKNOWN_FAILURE,
       "Invalid operation on quiesced store")

  method is_closed () =
    _closed

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

  method private _wrap_exception2 name (f:unit -> 'a) =
    try
      f ()
    with
        | Failure s ->
          begin
            Lwt_log.ign_debug_f "%s Failure %s: => FOOBAR? %b" name s _closed;
            if _closed
            then raise (Common.XException (Arakoon_exc.E_GOING_DOWN, s ^ " : database closed"))
            else raise Server.FOOBAR
          end
        | exn -> raise exn

  method private _with_tx : 'a. transaction -> (Camltc.Hotc.bdb -> 'a Lwt.t) -> 'a Lwt.t =
    fun tx f ->
      match _tx with
        | None -> failwith "not in a transaction"
        | Some (tx', db) ->
            if tx != tx'
            then
              failwith "the provided transaction is not the current transaction of the store"
            else
              f db

  method private _with_tx2 : 'a. transaction -> (Camltc.Hotc.bdb -> 'a) -> 'a =
    fun tx f ->
      match _tx with
        | None -> failwith "not in a transaction"
        | Some (tx', db) ->
            if tx != tx'
            then
              failwith "the provided transaction is not the current transaction of the store"
            else
              f db

  method private _update_in_tx : 'a. transaction -> (Camltc.Hotc.bdb -> 'a Lwt.t) -> 'a Lwt.t =
    fun tx f ->
      self # _with_tx tx f

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
    let t0 = Unix.gettimeofday() in
    Lwt.finalize
      (fun () ->
        Camltc.Hotc.transaction db
          (fun db ->
            let tx = new transaction in
            _tx <- Some (tx, db);
            f tx >>= fun a ->
            Lwt.return a))
      (fun () ->
        let t = ( Unix.gettimeofday() -. t0) in
        if t > 1.0
        then
          Lwt_log.info_f "Tokyo cabinet transaction took %fs" t
        else
          Lwt.return (); >>= fun () ->
        _tx <- None; Lwt.return ())

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

  method exists key =
    let bdb = Camltc.Hotc.get_bdb db in
    let r = B.exists bdb key in
    Lwt.return r

  method get key =
    let bdb = Camltc.Hotc.get_bdb db in
    B.get bdb key

  method range prefix first finc last linc max =
    let bdb = Camltc.Hotc.get_bdb db in
    let r = B.range bdb (_f prefix first) finc (_l prefix last) linc max in
    filter_keys_array r

  method range_entries prefix first finc last linc max =
    let bdb = Camltc.Hotc.get_bdb db in
    let r = _range_entries prefix bdb (_f prefix first) finc (_l prefix last) linc max in
    r

  method rev_range_entries prefix first finc last linc max =
    let bdb = Camltc.Hotc.get_bdb db in
    let r = B.rev_range_entries prefix bdb (_f prefix first) finc (_l prefix last) linc max in
    r

  method prefix_keys prefix max =
    let bdb = Camltc.Hotc.get_bdb db in
    let keys_array = B.prefix_keys bdb prefix max in
	let keys_list = filter_keys_array keys_array in
	Lwt.return keys_list

  method set tx key value =
    self # _wrap_exception2 "set"
      (fun () -> self # _with_tx2 tx
        (fun db -> _set db key value))

  method delete tx key =
    self # _wrap_exception2 "delete"
      (fun () -> self # _with_tx2 tx
        (fun db -> _delete db key) )

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
        let c = _delete_prefix db prefix in 
        Lwt.return c)

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
    Lwt_log.debug "local_store::reopen Hotc::reopen succeeded"

  method set_interval tx interval =
    Lwt_log.debug_f "set_interval %s" (Interval.to_string interval)
    >>= fun () ->
    self # _update_in_tx tx
      (fun db ->
	    _set_interval db interval;
	    _interval <- interval;
	    Lwt.return ()
      )

  method get_interval () = _interval

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

  method get_key_count () =
    Lwt_log.debug "local_store::get_key_count" >>= fun () ->
    Camltc.Hotc.transaction db (fun db -> Lwt.return ( B.get_key_count db ) )

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
  >>= fun (db, interval, routing_o, mlo) ->
  let store = new local_store db_name db interval routing_o mlo in
  Lwt.return (make_store (store :> simple_store))
