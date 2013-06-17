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
open Routing
open Log_extra
open Store
open Unix.LargeFile

exception BdbFFatal of string

module B = Camltc.Bdb

type t = { db : Camltc.Hotc.t;
           mutable location : string;
           mutable _tx : (transaction * Camltc.Hotc.bdb) option;
         }

let _get bdb key = B.get bdb key

let _set bdb key value = B.put bdb key value

let _delete bdb key    = B.out bdb key

let _delete_prefix bdb prefix =
  B.delete_prefix bdb prefix

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

let copy_store2 old_location new_location overwrite =
  File_system.exists old_location >>= fun src_exists ->
  if not src_exists
  then
    Logger.info_f_ "File at %s does not exist" old_location >>= fun () ->
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
        Logger.info_f_ "Not relocating store from %s to %s, destination exists" old_location new_location
      else
        File_system.copy_file old_location new_location
    end
  end

let safe_create db_path mode =
  Camltc.Hotc.create db_path ~mode [B.BDBTLARGE] >>= fun db ->
  let flags = Camltc.Bdb.flags (Camltc.Hotc.get_bdb db) in
  if List.mem Camltc.Bdb.BDBFFATAL flags
    then Lwt.fail (BdbFFatal db_path)
    else Lwt.return db

let get_construct_params db_name ~mode =
  safe_create db_name mode

let _with_tx ls tx f =
  match ls._tx with
    | None -> failwith "not in a local store transaction, _with_tx"
    | Some (tx', db) ->
        if tx != tx'
        then
          failwith "the provided transaction is not the current transaction of the local store"
        else
          f db

let _tranbegin ls =
  let tx = new transaction in
  let bdb = Camltc.Hotc.get_bdb ls.db in
  ls._tx <- Some (tx, bdb);
  Camltc.Bdb._tranbegin bdb;
  tx

let _trancommit ls =
  match ls._tx with
    | None -> failwith "not in a local store transaction, _trancommit"
    | Some (tx, bdb) ->
        Camltc.Bdb._trancommit bdb


let _tranabort ls =
  match ls._tx with
    | None -> failwith "not in a local store transaction, _tranabort"
    | Some (tx, bdb) ->
        Camltc.Bdb._tranabort bdb;
        ls._tx <- None

let with_transaction ls f =
  let t0 = Unix.gettimeofday() in
  Lwt.finalize
    (fun () ->
      Camltc.Hotc.transaction ls.db
        (fun db ->
          let tx = new transaction in
          ls._tx <- Some (tx, db);
          f tx >>= fun a ->
          Lwt.return a))
    (fun () ->
      let t = ( Unix.gettimeofday() -. t0) in
      if t > 1.0
      then
        Logger.info_f_ "Tokyo cabinet transaction took %fs" t
      else
        Lwt.return (); >>= fun () ->
      ls._tx <- None; Lwt.return ())

let defrag ls =
  Logger.info_ "local_store :: defrag" >>= fun () ->
  Camltc.Hotc.defrag ls.db >>= fun rc ->
  Logger.info_f_ "local_store %s :: defrag done: rc=%i" ls.location rc >>= fun () ->
  Lwt.return ()

let exists ls key =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  B.exists bdb key

let get ls key =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  B.get bdb key

let range ls prefix first finc last linc max =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  let r = B.range bdb (_f prefix first) finc (_l prefix last) linc max in
  filter_keys_array r

let range_entries ls prefix first finc last linc max =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  let r = _range_entries prefix bdb first finc last linc max in
  r

let rev_range_entries ls prefix first finc last linc max =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  let r = B.rev_range_entries prefix bdb first finc last linc max in
  r

let prefix_keys ls prefix max =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  let keys_array = B.prefix_keys bdb prefix max in
  let keys_list = filter_keys_array keys_array in
  keys_list

let set ls tx key value =
  _with_tx ls tx (fun db -> _set db key value)

let delete ls tx key =
  _with_tx ls tx (fun db -> _delete db key)

let delete_prefix ls tx prefix =
  _with_tx ls tx
    (fun db -> _delete_prefix db prefix)

let close ls =
  Camltc.Hotc.close ls.db >>= fun () ->
  Logger.info_f_ "local_store %S :: closed  () " ls.location >>= fun () ->
  Lwt.return ()

let get_location ls = Camltc.Hotc.filename ls.db

let reopen ls f quiesced =
  let mode =
    begin
      if quiesced then
        B.readonly_mode
      else
        B.default_mode
    end in
  Logger.info_f_ "local_store %S::reopen calling Hotc::reopen" ls.location >>= fun () ->
  Camltc.Hotc.reopen ls.db f mode >>= fun () ->
  Logger.info_ "local_store::reopen Hotc::reopen succeeded"

let get_key_count ls =
  Logger.debug_ "local_store::get_key_count" >>= fun () ->
  Camltc.Hotc.transaction ls.db (fun db -> Lwt.return ( B.get_key_count db ) )

let copy_store ls networkClient (oc: Lwt_io.output_channel) =
  let db_name = ls.location in
  let stat = Unix.LargeFile.stat db_name in
  let length = stat.st_size in
  Logger.info_f_ "store:: copy_store (filesize is %Li bytes)" length >>= fun ()->
  begin
    if networkClient
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

let optimize ls quiesced =
  let db_optimal = ls.location ^ ".opt" in
  Logger.info_ "Copying over db file" >>= fun () ->
  Lwt_io.with_file
    ~flags:[Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC]
    ~mode:Lwt_io.output
    db_optimal (fun oc -> copy_store ls false oc )
  >>= fun () ->
  begin
    Logger.info_f_ "Creating new db object at location %s" db_optimal >>= fun () ->
    safe_create db_optimal Camltc.Bdb.default_mode >>= fun db_opt ->
    Lwt.finalize
      ( fun () ->
        Logger.info_ "Optimizing db copy" >>= fun () ->
        Camltc.Hotc.optimize db_opt >>= fun () ->
        Logger.info_ "Optimize db copy complete"
      )
      ( fun () ->
        Camltc.Hotc.close db_opt
      )
  end >>= fun () ->
  File_system.rename db_optimal ls.location >>= fun () ->
  reopen ls (fun () -> Lwt.return ()) quiesced

let relocate ls new_location =
  copy_store2 ls.location new_location true >>= fun () ->
  let old_location = ls.location in
  let () = ls.location <- new_location in
  Logger.info_f_ "Attempting to unlink file '%s'" old_location >>= fun () ->
  File_system.unlink old_location >>= fun () ->
  Logger.info_f_ "Successfully unlinked file at '%s'" old_location


let get_fringe ls border direction =
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
                    Lwt_extra.ignore_result ( Logger.debug_f_ "Not skipping key: %s" k )
                  else
                    begin
                      Lwt_extra.ignore_result ( Logger.debug_f_ "Skipping key: %s" k );
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
  Logger.debug_f_ "local_store::get_fringe %S" (Log_extra.string_option2s border) >>= fun () ->
  let buf = Buffer.create 128 in
  Lwt.finalize
    (fun () ->
      Camltc.Hotc.transaction ls.db
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
      Logger.debug_f_ "buf:%s" (Buffer.contents buf)
    )

let make_store read_only db_name =
  let mode =
    if read_only
    then B.readonly_mode
    else B.default_mode
  in
  Logger.info_f_ "Creating local store at %s" db_name >>= fun () ->
  get_construct_params db_name ~mode
  >>= fun db ->
  Lwt.return { db = db;
               location = db_name;
               _tx = None; }

