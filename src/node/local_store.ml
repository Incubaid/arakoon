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

open Std
open Mp_msg
open Lwt
open Update
open Routing
open Log_extra
open Simple_store
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


let copy_store2 old_location new_location overwrite =
  File_system.exists old_location >>= fun src_exists ->
  if not src_exists
  then
    Logger.info_f_ "File at %s does not exist" old_location >>= fun () ->
    raise Not_found
  else
    File_system.copy_file old_location new_location overwrite

let safe_create ?(lcnum=1024) ?(ncnum=512) db_path ~mode  =
  Camltc.Hotc.create db_path ~mode ~lcnum ~ncnum [B.BDBTLARGE] >>= fun db ->
  let flags = Camltc.Bdb.flags (Camltc.Hotc.get_bdb db) in
  if List.mem Camltc.Bdb.BDBFFATAL flags
  then Lwt.fail (BdbFFatal db_path)
  else Lwt.return db


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
      let t0 = Unix.gettimeofday () in
      Camltc.Bdb._trancommit bdb;
      let t = ( Unix.gettimeofday () -. t0) in
      if t > 1.0
      then
        Lwt.ignore_result (
          let fn = Camltc.Hotc.filename ls.db in
          Logger.info_f_ "Tokyo cabinet (%s) transaction commit took %fs" fn t )


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
       begin
         if t > 1.0
         then
           let fn = Camltc.Hotc.filename ls.db in
           Logger.info_f_ "Tokyo cabinet (%s) transaction took %fs" fn t
         else
           Lwt.return ()
       end
       >>= fun () ->
       ls._tx <- None;
       Lwt.return ())

let defrag ls =
  Logger.info_ "local_store :: defrag" >>= fun () ->
  let bdb = Camltc.Hotc.get_bdb ls.db in
  Lwt_preemptive.detach
    (Camltc.Bdb.defrag ~step:0L)
    bdb
  >>= fun rc ->
  Logger.info_f_ "local_store %s :: defrag done: rc=%i" ls.location rc >>= fun () ->
  Lwt.return ()

let exists ls key =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  B.exists bdb key

let get ls key =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  B.get bdb key

let set ls tx key value =
  _with_tx ls tx (fun db -> _set db key value)

let delete ls tx key =
  _with_tx ls tx (fun db -> _delete db key)

let delete_prefix ls tx prefix =
  _with_tx ls tx
    (fun db -> _delete_prefix db prefix)

let flush ls =
  Lwt.return ()

let close ls flush =
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


type cursor = (B.bdb * B.bdbcur)

let with_cursor ls f =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  B.with_cursor bdb (fun _ cur -> f (bdb, cur))

let wrap_not_found f =
  try
    f ();
    true
  with Not_found -> false

let cur_last (bdb, cur) =
  wrap_not_found (fun () -> B.last bdb cur)

let cur_get (bdb, cur) = (B.key bdb cur, B.value bdb cur)
let cur_get_key (bdb, cur) = B.key bdb cur
let cur_get_value (bdb, cur) = B.value bdb cur

let cur_prev (bdb, cur) =
  wrap_not_found
    (fun () ->
     B.prev bdb cur)

let cur_next (bdb, cur) =
  wrap_not_found
    (fun () ->
     B.next bdb cur)

let cur_jump (bdb, cur) ?(direction=Right) key =
  match direction with
  | Right ->
     wrap_not_found
       (fun () ->
        B.jump bdb cur key)
  | Left ->
     try
       B.jump bdb cur key;
       if String.(=:) key (B.key bdb cur)
       then
         true
       else
         begin
           try
             B.prev bdb cur;
             true
           with Not_found ->
             false
         end
     with Not_found ->
       cur_last (bdb, cur)

let make_store ~lcnum ~ncnum read_only db_name  =
  let mode =
    if read_only
    then B.readonly_mode
    else B.default_mode
  in
  Logger.info_f_
    "Creating local store at %s (lcnum=%i) (ncnum=%i)" db_name
    lcnum ncnum >>= fun () ->
  safe_create db_name ~mode ~lcnum ~ncnum
  >>= fun db ->
  Lwt.return { db = db;
               location = db_name;
               _tx = None; }
