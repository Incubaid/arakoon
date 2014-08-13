(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

open Std
open Lwt
open Simple_store
open Unix.LargeFile

exception BdbFFatal of string

module B = Camltc.Bdb

type t = { db : Camltc.Hotc.t;
           mutable location : string;
           mutable _tx : (transaction * Camltc.Hotc.bdb) option;
           lcnum : int;
           ncnum : int;
         }

let range ls first finc last linc max =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  B.range bdb (Some first) finc last linc max


let copy_store2 old_location new_location
                ~overwrite ~throttling =
  File_system.exists old_location >>= fun src_exists ->
  if not src_exists
  then
    Logger.info_f_ "File at %s does not exist" old_location >>= fun () ->
    raise Not_found
  else
    File_system.copy_file old_location new_location ~overwrite ~throttling

let safe_create ?(lcnum=1024) ?(ncnum=512) db_path ~mode  =
  Camltc.Hotc.create db_path ~mode ~lcnum ~ncnum [B.BDBTLARGE] >>= fun db ->
  let flags = B.flags (Camltc.Hotc.get_bdb db) in
  if List.mem B.BDBFFATAL flags
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
  B._tranbegin bdb;
  tx

let _trancommit ls =
  match ls._tx with
    | None -> failwith "not in a local store transaction, _trancommit"
    | Some (_tx, bdb) ->
      let t0 = Unix.gettimeofday () in
      B._trancommit bdb;
      let t = ( Unix.gettimeofday () -. t0) in
      if t > 1.0
      then
        Lwt.ignore_result (
          let fn = Camltc.Hotc.filename ls.db in
          Logger.info_f_ "Tokyo cabinet (%s) transaction commit took %fs" fn t )


let _tranabort ls =
  match ls._tx with
    | None -> failwith "not in a local store transaction, _tranabort"
    | Some (_tx, bdb) ->
      B._tranabort bdb;
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
    (B.defrag ~step:0L)
    bdb
  >>= fun rc ->
  Logger.info_f_ "local_store %s :: defrag done: rc=%i" ls.location rc >>= fun () ->
  Lwt.return ()

let exists ls key =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  B.exists bdb key

let get ls key =
  let bdb = Camltc.Hotc.get_bdb ls.db in
  B.get3 bdb key

let set ls tx key value =
  _with_tx ls tx (fun db -> B.put db key value)

let put ls tx key = function
  | None ->
     begin
       try
         _with_tx ls tx (fun db -> B.out db key)
       with Not_found -> ()
     end
  | Some v ->
     set ls tx key v

let delete_prefix ls tx prefix =
  _with_tx ls tx
    (fun db -> B.delete_prefix db prefix)

let flush _ls =
  Lwt.return ()

let close ls ~flush ~sync =
  ignore flush;
  if sync
  then
    Camltc.Hotc.sync ls.db;
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
  Logger.ign_debug_ "local_store::get_key_count";
  let bdb = Camltc.Hotc.get_bdb ls.db in
  B.get_key_count bdb

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


let relocate ls new_location =
  copy_store2 ls.location new_location ~overwrite:true
              ~throttling:0.0
  >>= fun () ->
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

let cur_get (bdb, cur) = (B.key3 bdb cur, B.value3 bdb cur)
let cur_get_key (bdb, cur) = B.key3 bdb cur
let cur_get_value (bdb, cur) = B.value3 bdb cur

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
               _tx = None;
               ncnum;
               lcnum;}


let optimize ls ~quiesced ~stop =
  let open Camltc in
  let batch_size = 10_000 in
  let log_size = 1_000_000 in
  let log_batch_count = (log_size / batch_size) - 1 in
  let db_optimal = ls.location ^ ".opt" in
  File_system.unlink db_optimal >>= fun () ->
  Logger.info_f_ "Creating new db object at location %s" db_optimal >>= fun () ->
  make_store ~lcnum:ls.lcnum ~ncnum:ls.ncnum false db_optimal >>= fun ls_optimal ->
  Lwt.finalize
    (fun () ->
     let target = Camltc.Hotc.get_bdb ls_optimal.db in
     let go () =
       Bdb.with_cursor
         (Camltc.Hotc.get_bdb ls.db)
         (fun source cursor ->
          Bdb.first source cursor;
          let max = Some batch_size in
          let rec loop i =
            let count =
              Bdb.copy_from_cursor
                ~source
                ~cursor
                ~target
                ~max in
            if !stop
            then
              false
            else if count = batch_size
            then
              begin
                if i = log_batch_count
                then
                  begin
                    Logger.ign_info_f_ "Copied %i keys to optimized db %s" log_size db_optimal;
                    loop 0
                  end
                else
                  loop (i + 1)
              end
            else
              begin
                Camltc.Hotc.sync_nolock ls_optimal.db;
                true
              end
          in
          loop 0)
     in
     Lwt_preemptive.detach go ())
    (fun () ->
     Camltc.Hotc.close ls_optimal.db) >>= fun finished ->
  Logger.info_f_ "Optimize db copy complete (finished=%b)" finished >>= fun () ->
  reopen
    ls
    (fun () ->
     if finished
     then
       File_system.rename db_optimal ls.location
     else
       File_system.unlink db_optimal)
    quiesced >>= fun () ->
  Lwt.return finished
