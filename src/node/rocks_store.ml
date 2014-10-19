open Std
open Rocks
open Lwt
open Simple_store

module Rocks_key_value_store = struct
  type t = {
    db : RocksDb.t;
    location : string;
    mutable _tx : (transaction * WriteBatch.t) option;
  }

  let get_location t = t.location

  let make_store ~lcnum ~ncnum b location =
    ignore lcnum; ignore ncnum; ignore b;
    let options = Options.create () in
    Options.set_create_if_missing options true;
    let db = RocksDb.open_db options location in
    Lwt.return { db; location; _tx = None }

  let reopen t f b =
    ignore (t, f, b);
    Lwt.return ()
  let close t ~flush ~sync =
    ignore (flush, sync);
    RocksDb.close t.db;
    Lwt.return ()

  let get' t key =
    let ro = ReadOptions.create () in
    RocksDb.get t.db ro key
  let get_exn t key =
    match get' t key with
      | Some v -> v
      | None -> raise Not_found
  let get = get_exn

  let exists t key = get' t key <> None

  type cursor = Iterator.t

  let with_cursor t f =
    let ro = ReadOptions.create () in
    let iterator = Iterator.create t.db ro in
    f iterator

  let with_valid f c =
    f c;
    Iterator.is_valid c

  let cur_last = with_valid (fun c -> Iterator.seek_to_last c)

  let cur_get_key = Iterator.get_key
  let cur_get_value = Iterator.get_value
  let cur_get c = cur_get_key c, cur_get_value c

  let cur_prev = with_valid (fun c -> Iterator.prev c)
  let cur_next = with_valid (fun c -> Iterator.next c)
  let cur_jump c ?(direction = Right) key =
    Iterator.seek c key;
    if Iterator.is_valid c
    then begin
      match direction with
      | Right -> true
      | Left -> begin
          (* maybe we jumped to far... *)
          if cur_get_key c = key
          then true
          else cur_prev c
        end
    end else begin
      match direction with
      | Right -> false
      | Left -> cur_last c
    end

  let with_transaction t f =
    let wb = WriteBatch.create () in
    let tx = new transaction in
    t._tx <- Some (tx, wb);
    Lwt.finalize
      (fun () ->
         f tx >>= fun res ->
         RocksDb.write (WriteOptions.create ()) t.db wb;
         Lwt.return res)
      (fun () ->
         t._tx <- None;
         Lwt.return ())

  let put t tx key vo =
    ignore tx;
    let wb = match t._tx with
      | None -> failwith "error not in transcation"
      | Some (_tx, wb) -> wb in
    match vo with
    | None -> WriteBatch.delete wb key
    | Some v -> WriteBatch.put wb key v

  let set t tx key value =
    put t tx key (Some value)

  let copy_store2 s1 s2 ~overwrite ~throttling =
    ignore (s1, s2, overwrite, throttling);
    failwith "not implemented"

  let copy_store t b oc =
    ignore (t, b, oc);
    failwith "not implemented"

  let defrag t =
    ignore t;
    failwith "not implemented"

  let optimize t ~quiesced ~stop =
    ignore (t, quiesced, stop);
    failwith "not implemented"

  let get_key_count _t = failwith "not supported"

  let relocate _t _s = failwith "not supported"

  let flush _t = Lwt.return ()
end
