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

open Tlogcollection
open Tlogcommon
open Update
open Lwt
open Unix.LargeFile
open Lwt_buffer
open Tlogreader2

exception TLCCorrupt of (Int64.t * Sn.t)
exception TLCNotProperlyClosed of string

let section = Logger.Section.main

let file_regexp = Str.regexp "^[0-9]+\\.tl\\(og\\|f\\|c\\)$"
let file_name c = Printf.sprintf "%03i.tlog" c
let archive_extension = ".tlf"
let archive_name c = Printf.sprintf "%03i.tlf" c
let head_fname = "head.db"

let head_name () = head_fname

let get_file_number i = Sn.div i (Sn.of_int !Tlogcommon.tlogEntriesPerFile)

let to_archive_name fn =
  let length = String.length fn in
  let extension = String.sub fn (length -5) 5 in
  if extension = ".tlog"
  then
    let root = String.sub fn 0 (length -5) in
    root ^ archive_extension
  else failwith (Printf.sprintf "extension is '%s' and should be '.tlog'" extension)


let to_tlog_name fn =
  let length = String.length fn in
  let extension = String.sub fn (length -4) 4 in
  if extension = archive_extension || extension = ".tlc"
  then
    let root = String.sub fn 0 (length -4) in
    root ^ ".tlog"
  else failwith (Printf.sprintf "to_tlog_name:extension is '%s' and should be '%s'" extension archive_extension)

let get_number fn =
  let dot_pos = String.index fn '.' in
  let pre = String.sub fn 0 dot_pos in
  int_of_string pre

let get_tlog_names tlog_dir =
  Lwt.catch
    (fun () ->
      File_system.lwt_directory_list tlog_dir
    )
    (function
      | Unix.Unix_error(Unix.EIO, fn, _) as exn ->
          if fn = "readdir"
          then Lwt.fail (Node_cfg.InvalidTlogDir tlog_dir)
          else Lwt.fail exn
      | exn -> Lwt.fail exn
    ) >>= fun entries ->
  let filtered = List.filter
    (fun e -> Str.string_match file_regexp e 0)
    entries
  in
  let my_compare fn1 fn2 =
    compare (get_number fn1) (get_number fn2)
  in
  let sorted = List.sort my_compare filtered in
  let filtered2 = List.fold_left
    (fun acc name ->
      match acc with
	| [] -> name :: acc
	| prev :: rest ->
	  if get_number prev = get_number name (* both x.tlf and x.tlog present *)
	  then prev :: rest (* prefer .tlf over .tlog *)
	  else name :: acc
    ) [] sorted
  in
  let sorted2 = List.rev filtered2 in
  let log_e e = Logger.debug_f_ "entry %s %i" e (get_number e) in
  Lwt_list.iter_s log_e sorted2 >>= fun () ->
  Lwt.return sorted2

let extension_of filename =
  let lm = String.length filename - 1 in
  let dot_pos = String.rindex_from filename lm '.' in
  let len = lm - dot_pos +1 in
  String.sub filename dot_pos len

let folder_for filename index =
  let extension = extension_of filename in
  if extension = ".tlog"
  then Tlogreader2.AU.fold, extension, index
  else if extension = archive_extension then Tlogreader2.C.fold, extension, None
  else if extension = ".tlc" then Tlogreader2.O.fold, extension, None
  else failwith (Printf.sprintf "no folder for '%s'" extension)


let fold_read tlog_dir file_name
    ~index
    (lowerI:Sn.t)
    (too_far_i:Sn.t option)
    ~first
    (a0:'a)
    (f:'a -> Entry.t -> 'a Lwt.t) =
  let full_name = Filename.concat tlog_dir file_name in
  let folder, extension, index' = folder_for file_name index in
  Logger.debug_f_ "fold_read extension=%s => index':%s" extension (Index.to_string index') >>= fun () ->
  let ic_f ic = folder ic ~index:index' lowerI too_far_i ~first a0 f in
  Lwt.catch
    (fun () -> Lwt_io.with_file ~mode:Lwt_io.input full_name ic_f)
    (function
      | Unix.Unix_error(_,"open",_) as exn ->
	    if extension = ".tlog"
	    then (* file might have moved meanwhile *)
	      begin
	        Logger.debug_f_ "%s became %s meanwhile " file_name archive_extension
	        >>= fun () ->
	        let an = to_archive_name file_name in
	        let full_an = Filename.concat tlog_dir an in
	        Logger.debug_f_ "folding compressed %s" an >>= fun () ->
	        Lwt_io.with_file ~mode:Lwt_io.input full_an
	          (fun ic ->
                let index = None in
                Tlogreader2.AC.fold ic ~index lowerI too_far_i ~first a0 f)
	      end
	    else
	      Lwt.fail exn
      | exn -> Lwt.fail exn
    )

type validation_result = (Entry.t option * Index.index)

let _make_close_marker node_id = Some ("closed:" ^ node_id)
let _make_open_marker node_id =  Some ("opened:" ^ node_id)

let _validate_one tlog_name node_id ~check_marker : validation_result Lwt.t =
  Logger.debug_f_ "Tlc2._validate_one %s" tlog_name >>= fun () ->
  let e2s e = let i = Entry.i_of e in Printf.sprintf "(%s,_)" (Sn.string_of i) in
  let prev_entry = ref None in
  let new_index = Index.make tlog_name in
  Lwt.catch
    (fun () ->
      let first = Sn.of_int 0 in
      let folder, _, index = folder_for tlog_name None in

      let do_it ic = folder ic ~index Sn.start None ~first None
	    (fun a0 entry ->
          let () = Index.note entry new_index in
          let r = Some entry in
          let () = prev_entry := r in
          Lwt.return r)
      in
      Lwt_io.with_file tlog_name ~mode:Lwt_io.input do_it
      >>= fun eo ->
      begin
        if not check_marker
        then Lwt.return eo
        else
          let eo' =
            match eo with
              | None -> None
              | Some e ->
                  let s = _make_close_marker node_id in
                  if Entry.check_marker e s
                  then !prev_entry
                  else raise (TLCNotProperlyClosed (Entry.entry2s e))
          in
          Lwt.return eo'
      end
      >>= fun eo' ->
      Logger.debug_f_ "XX:a=%s" (Log_extra.option2s e2s eo') >>= fun () ->
      Logger.debug_f_ "After validation index=%s" (Index.to_string new_index) >>= fun () ->
      Lwt.return (eo', new_index)
    )
    (function
      | Unix.Unix_error(Unix.ENOENT,_,_) -> Lwt.return (None, new_index)
      | Tlogcommon.TLogCheckSumError pos ->
        begin
          match !prev_entry with
            | Some entry -> let i = Entry.i_of entry in Lwt.fail (TLCCorrupt (pos,i))
            | None -> Lwt.fail (TLCCorrupt(pos, Sn.start))
        end
      | exn -> Lwt.fail exn
    )


let _validate_list tlog_names node_id ~check_marker=
  Lwt_list.fold_left_s (fun _ tn -> _validate_one tn node_id ~check_marker) (None,None) tlog_names >>= fun (eo,index) ->
  Logger.info_f_ "_validate_list %s => %s" (String.concat ";" tlog_names) (Index.to_string index) >>= fun () ->
  Lwt.return (TlogValidIncomplete, eo, index)




let validate_last tlog_dir node_id ~check_marker=
  Logger.debug_f_ "validate_last ~check_marker:%b" check_marker >>= fun () ->
  get_tlog_names tlog_dir >>= fun tlog_names ->
  match tlog_names with
    | [] -> _validate_list [] node_id ~check_marker
    | _ ->
      let n = List.length tlog_names in
      let last = List.nth tlog_names (n-1) in
      let fn = Filename.concat tlog_dir last in
      _validate_list [fn] node_id ~check_marker>>= fun r ->
      let (validity, last_eo, index) = r in
      match last_eo with
        | None ->
          begin
            if n > 1
            then
              let prev_last = List.nth tlog_names (n-2) in
              let last_non_empty = Filename.concat tlog_dir prev_last in
              _validate_list [last_non_empty] node_id ~check_marker
            else
              Lwt.return r
          end
        | Some i -> Lwt.return r



let get_count tlog_names =
  match List.length tlog_names with
    | 0 -> 0
    | n -> let last = List.nth tlog_names (n-1) in
	   let length = String.length last in
	   let postfix = String.sub last (length -4) 4 in
	   let number = get_number last in
	   if postfix = "tlog"
       then number
	   else number + 1

module F = struct
  type t = {oc:Lwt_io.output Lwt_io.channel;
            oc_start: int64;
            fd:Lwt_unix.file_descr;
            fn:string;
            c: int;}


  let make oc fd fn c oc_start = {oc;fd;fn;c;oc_start}
  let file_pos t = Int64.add (Lwt_io.position t.oc) t.oc_start
      (* the channel always starts at pos 0, even if it's a file that was opened in append mode *)
  let fn_of t = t.fn
  let oc_of t = t.oc
  let fsync t = Lwt_unix.fsync t.fd
  let close t = Lwt_io.close t.oc >>= fun () -> Logger.info_f_ "closed %s" t.fn
end


let _init_file tlog_dir c =
  let fn = file_name c in
  let full_name = Filename.concat tlog_dir fn in
  Lwt_unix.openfile full_name [Unix.O_CREAT;Unix.O_APPEND;Unix.O_WRONLY] 0o644 >>= fun fd ->
  Lwt_unix.LargeFile.fstat fd >>= fun stats ->
  let pos0 = stats.st_size in
  let oc = Lwt_io.of_fd ~mode:Lwt_io.output fd in
  Logger.debug_f_ "_init_file pos0 : %Li" pos0 >>= fun () ->
  let f = F.make oc fd fn c pos0 in
  Lwt.return f


let iterate_tlog_dir tlog_dir ~index start_i too_far_i f =
  let tfs = Sn.string_of too_far_i in
  Logger.debug_f_ "Tlc2.iterate_tlog_dir tlog_dir:%s start_i:%s too_far_i:%s ~index:%s"
    tlog_dir (Sn.string_of start_i) tfs (Index.to_string index)
  >>= fun () ->
  get_tlog_names tlog_dir >>= fun tlog_names ->
  let acc_entry (i0:Sn.t) entry = f entry >>= fun () -> let i = Entry.i_of entry in Lwt.return i
  in
  let num_tlogs = List.length tlog_names in
  let maybe_fold acc fn =
    let cnt,low = acc in
    let factor = Sn.of_int (!Tlogcommon.tlogEntriesPerFile) in
    let linf = Sn.of_int (get_number fn) in
    let ( * ) = Sn.mul in
    let test0 = linf * factor in
    let test1 = (Sn.succ linf) * factor in
    let low_s = Sn.string_of low in
    let lowp = Sn.succ low in
    let lowp_s = Sn.string_of lowp in
    let test_result =
      (test0 <= lowp) &&
	    (low   <  test1) &&
	    low <= too_far_i
    in
    Logger.debug_f_ "%s <?= lowp:%s &&  low:%s <? %s && (low:%s <?= %s) yields:%b"
      (Sn.string_of test0 ) lowp_s
      low_s (Sn.string_of test1)
      low_s tfs
      test_result
    >>= fun () ->
    if test_result
    then
      begin
        Logger.debug_f_ "fold_read over: %s (test0=%s)" fn
	      (Sn.string_of test0) >>= fun () ->
        let first = test0 in
        Logger.info_f_ "Replaying tlog file: %s (%d/%d)" fn cnt num_tlogs  >>= fun () ->
        let t1 = Sys.time () in
        fold_read tlog_dir fn ~index low (Some too_far_i) ~first low acc_entry >>= fun x ->
        Logger.info_f_ "Completed replay of %s, took %f seconds, %i to go" fn (Sys.time () -. t1) (num_tlogs - cnt) >>= fun () ->
        Lwt.return (cnt+1,x)
      end
    else Lwt.return (cnt+1,low)
  in
  Lwt_list.fold_left_s maybe_fold (1,start_i) tlog_names
  >>= fun x ->
  Lwt.return ()





class tlc2 (tlog_dir:string) (new_c:int)
  (last:Entry.t option) (index:Index.index) (use_compression:bool)
  (node_id:string) (fsync:bool)
  =
  let inner =
    match last with
      | None -> 0
      | Some e ->
        let i = Entry.i_of e in
	    let step = (Sn.of_int !Tlogcommon.tlogEntriesPerFile) in
	    (Sn.to_int (Sn.rem i step)) + 1
  in
object(self: # tlog_collection)
  val mutable _file = (None:F.t option)
  val mutable _index = (None: Index.index)
  val mutable _inner = inner (* ~ pos in file *)
  val mutable _outer = new_c (* ~ pos in dir *)
  val mutable _previous_entry = last
  val mutable _compression_q = Lwt_buffer.create_fixed_capacity 5
  val mutable _compressing = false
  val mutable _closed = false
  val _jc = (Lwt_condition.create () : unit Lwt_condition.t)
  initializer self # start_compression_loop ()


  method validate_last_tlog () =
    validate_last tlog_dir node_id ~check_marker:false >>= fun r ->
    let (validity, entry, new_index) = r in
    let tlu = Filename.concat tlog_dir (file_name _outer) in
    let matches = Index.match_filename tlu new_index in
    Logger.debug_f_ "tlu=%S new_index=%s index=%s => matches=%b"
      tlu
      (Index.to_string new_index)
      (Index.to_string index)
      matches >>= fun () ->
    begin
      if matches
      then
        let () = _index <- new_index in
        Logger.info_ "replaced index"
      else
        Lwt.return ()
    end
    >>= fun () ->
    Lwt.return r


  method private start_compression_loop () =
    let rec loop () =
      Lwt_buffer.take _compression_q >>= fun job ->
      let () = _compressing <- true in
      let (tlu, tlc_temp, tlc) = job in
      let try_unlink_tlu () =
        Lwt.catch
          (fun () ->
            Logger.debug_f_ "unlink of %s" tlu >>= fun () ->
            Lwt_unix.unlink tlu >>= fun () ->
            Logger.debug_f_ "ok: unlinked %s" tlu
          )
          (fun exn -> Logger.warning_f_ ~exn "unlinking of %s failed" tlu) in
      File_system.exists tlc >>= fun tlc_exists ->
      if tlc_exists
      then
        begin
          Logger.info_f_ "Compression target %s already exists, this should be a complete .tlf" tlc >>= fun () ->
          try_unlink_tlu ()
        end
      else
        Lwt.catch
          (fun () ->
            Logger.debug_f_ "Compressing: %s into %s" tlu tlc_temp >>= fun () ->
            Compression.compress_tlog tlu tlc_temp  >>= fun () ->
            File_system.rename tlc_temp tlc >>= fun () ->
            try_unlink_tlu () >>= fun () ->
            Logger.debug_f_ "end of compress : %s -> %s" tlu tlc
          )
          (function exn -> Logger.warning_ "exception inside compression, continuing anyway")
         >>= fun () ->
      let () = _compressing <- false in
      let () = Lwt_condition.signal _jc () in
      loop ()
    in
    Lwt_extra.ignore_result (loop ())


  method log_value_explicit i value sync marker =
    if _closed
    then
      Logger.debug_f_ "logging when closed" >>= fun () ->
      Lwt.fail (Failure "CLOSING TLOG")
    else
      begin
        self # _prelude i >>= fun file ->
        let p = F.file_pos file in
        let oc = F.oc_of file in
        Tlogcommon.write_entry oc i value >>= fun () ->
        Lwt_io.flush oc >>= fun () ->
        begin
          if sync || fsync
          then F.fsync file
          else Lwt.return ()
        end
        >>= fun () ->
        let () = match _previous_entry with
          | None -> _inner <- _inner +1
          | Some pe->
            let pi = Entry.i_of pe in if pi < i then _inner <- _inner +1
        in
        let entry = Entry.make i value p marker in
        _previous_entry <- Some entry;
        Index.note entry _index;
        Lwt.return ()
      end

  method log_value i value =
    self # log_value_explicit i value fsync None

  method private _prelude i =
    let ( *: ) = Sn.mul in
    match _file with
      | None ->
	    begin
          Logger.debug_f_ "prelude %s" (Sn.string_of i) >>= fun () ->
	      let outer = Sn.div i (Sn.of_int !Tlogcommon.tlogEntriesPerFile) in
	      _outer <- Sn.to_int outer;
	      _init_file tlog_dir _outer >>= fun file ->
	      _file <- Some file;
          _index <- Index.make (F.fn_of file);
	      Lwt.return file
	    end
      | Some (file:F.t) ->
	    if i >= (Sn.of_int !Tlogcommon.tlogEntriesPerFile) *: (Sn.of_int (_outer + 1))
	    then
          let do_rotate() =
            let new_outer = _outer + 1 in
            Logger.info_f_ "i= %s & outer = %i => rotate to %i" (Sn.string_of i) _outer new_outer
            >>= fun () ->
            self # _rotate file new_outer
          in
          begin
            match _previous_entry with
              | Some pe when (Entry.i_of pe) = i -> Lwt.return file
              | _ -> do_rotate ()
          end
	    else
	      Lwt.return file


  method private _rotate file new_outer =
    assert (new_outer = _outer + 1);
    F.close file >>= fun () ->
    let old_outer = _outer in
    _inner <- 0;
    _outer <- new_outer;
    _init_file tlog_dir new_outer >>= fun new_file ->
    _file <- Some new_file;
    let index =
      let fn = file_name _outer in
      let full_name = Filename.concat tlog_dir fn in
      Index.make full_name
    in
    _index <- index;
    (* make compression job *)
    let tlu = Filename.concat tlog_dir (file_name old_outer) in
    let tlc = Filename.concat tlog_dir (archive_name old_outer) in
    let tlc_temp = tlc ^ ".part" in
    begin
      if use_compression
      then
        let job = (tlu,tlc_temp,tlc) in
        Lwt_buffer.add job _compression_q
      else
        Lwt.return ()
    end
    >>= fun () ->
    Lwt.return new_file



  method iterate (start_i:Sn.t) (too_far_i:Sn.t) (f:Entry.t -> unit Lwt.t) =
    let index = _index in
    Logger.debug_f_ "tlc2.iterate : index=%s" (Index.to_string index) >>= fun () ->
    iterate_tlog_dir tlog_dir ~index start_i too_far_i f


  method get_infimum_i () =
    get_tlog_names tlog_dir >>= fun names ->
    let f = !Tlogcommon.tlogEntriesPerFile in
    let v =
    match names with
      | [] -> Sn.of_int (-f)
      | h :: _ -> let n = Sn.of_int (get_number h) in
		          Sn.mul (Sn.of_int f) n
    in
    Logger.debug_f_ "Tlc2.infimum=%s" (Sn.string_of v) >>= fun () ->
    Lwt.return v

  method get_head_name () =
    Filename.concat tlog_dir (head_name())

  method dump_head oc =
    let head_name = self # get_head_name () in
    let stat = Unix.LargeFile.stat head_name in
    let length = stat.st_size in
    Logger.debug_f_ "tlcs:: dump_head (filesize is %Li bytes)" length >>= fun ()->
    Llio.output_int64 oc length >>= fun () ->
    Lwt_io.with_file
      ~flags:[Unix.O_RDONLY]
      ~mode:Lwt_io.input
      head_name (fun ic -> Llio.copy_stream ~length ~ic ~oc)
    >>= fun () ->
    Lwt_io.flush oc >>= fun () ->
    let module S = (val (Store.make_store_module (module Batched_store.Local_store))) in
    S.make_store head_name >>= fun store ->
    let io = S.consensus_i store in
    S.close store >>= fun () ->
    Logger.debug_f_ "head has consensus_i=%s" (Log_extra.option2s Sn.string_of io)
    >>= fun () ->
    let next_i = match io with
      | None -> Sn.start
      | Some i -> Sn.succ i
    in
    Lwt.return next_i

  method save_head ic =
    Logger.debug_ "save_head()" >>= fun () ->
    Llio.input_int64 ic >>= fun length ->
    let hf_name = self # get_head_name () in
    Lwt_io.with_file
      ~flags:[Unix.O_WRONLY;Unix.O_CREAT]
      ~mode:Lwt_io.output
      hf_name
      (fun oc -> Llio.copy_stream ~length ~ic ~oc )
    >>= fun () ->
    Logger.debug_ "done: save_head"


 method get_last_i () =
   match _previous_entry with
     | None -> Sn.start
     | Some pe -> let pi = Entry.i_of pe in pi

  method get_last_value i =
    match _previous_entry with
	  | None -> None
	  | Some pe ->
        let pi = Entry.i_of pe in
	    if pi = i
        then Some (Entry.v_of pe)
	    else
	      if i > pi
          then None
	      else (* pi > i *)
	        let msg = Printf.sprintf "get_last_value %s > %s can't look back so far"
		      (Sn.string_of pi) (Sn.string_of i)
	        in
	        failwith msg

  method get_last () =
    match _previous_entry with
      | None -> None
      | Some pe ->  Some (Entry.v_of pe, Entry.i_of pe)

  method close () =
    begin
      _closed <- true;
      Logger.debug_ "tlc2::close()" >>= fun () ->
      Lwt_buffer.wait_until_empty _compression_q >>= fun () ->
      begin
        if _compressing
        then Lwt_condition.wait _jc
        else Lwt.return ()
      end
      >>= fun () ->
      Logger.debug_ "tlc2::closes () (part2)" >>= fun () ->
      let last_file () =
        match _file with
          | None -> _init_file tlog_dir _outer
          | Some file -> Lwt.return file
      in
      last_file () >>= fun file ->
      begin
        match self # get_last() with
          | None -> Lwt.return ()
          | Some(v,i) ->
              begin
                let oc = F.oc_of file in
                let marker = _make_close_marker node_id in
                Tlogcommon.write_marker oc i v marker >>= fun () ->
                Logger.debug_f_ "wrote %S marker @i=%s for %S"
                  (Log_extra.string_option2s marker) (Sn.string_of i) node_id
              end
      end >>= fun () ->
      F.close file
    end


  method get_tlog_from_name n = Sn.of_int (get_number (Filename.basename n))

  method get_tlog_from_i = get_file_number

  method get_tlog_count () =
    get_tlog_names tlog_dir >>= fun tlogs ->
    Lwt.return (List.length tlogs)

  method dump_tlog_file start_i oc =
    let n = Sn.to_int (get_file_number start_i) in
    let an = archive_name n in
    let fn = file_name n  in
    begin
      File_system.exists (Filename.concat tlog_dir an) >>= function
	| true -> Lwt.return an
	| false -> Lwt.return fn
    end
    >>= fun name ->
    let canonical = Filename.concat tlog_dir name in
    Logger.debug_f_ "start_i = %Li => canonical=%s" start_i canonical >>= fun () ->
    Llio.output_string oc name >>= fun () ->
    File_system.stat canonical >>= fun stats ->
    let length = Int64.of_int(stats.Unix.st_size) in (* why don't they use largefile ? *)
    Lwt_io.with_file ~mode:Lwt_io.input canonical
      (fun ic ->
	Llio.output_int64 oc length >>= fun () ->
	Llio.copy_stream ~length ~ic ~oc
      )
    >>= fun () ->
    Logger.debug_f_ "dumped:%s (%Li) bytes" canonical length
    >>= fun () ->
    let next_i = Sn.add start_i (Sn.of_int !tlogEntriesPerFile) in
    Lwt.return next_i

  method save_tlog_file name length ic =
    (* what with rotation, open streams, ...*)
    let canon = Filename.concat tlog_dir name in
    let tmp = canon ^ ".tmp" in
    Logger.debug_f_ "save_tlog_file: %s" tmp >>= fun () ->
    Lwt_io.with_file ~mode:Lwt_io.output tmp (fun oc -> Llio.copy_stream ~length ~ic ~oc) >>= fun () ->
    File_system.rename tmp canon


  method remove_oldest_tlogs count =
    get_tlog_names tlog_dir >>= fun existing ->
    let rec remove_one l = function
      | 0 -> Lwt.return ()
      | n ->
        let oldest = Filename.concat tlog_dir (List.hd l) in
        Logger.debug_f_ "Unlinking %s" oldest >>= fun () ->
        File_system.unlink (oldest) >>= fun () ->
        remove_one (List.tl l) (n-1)
    in remove_one existing count

  method remove_below i =
    get_tlog_names tlog_dir >>= fun existing ->
    let maybe_remove fn =
      let n = get_number fn in
      let fn_stop = Sn.of_int ((n+1) * !Tlogcommon.tlogEntriesPerFile) in
      if fn_stop < i then
	begin
	  let canonical = Filename.concat tlog_dir fn in
	  Logger.debug_f_ "Unlinking %s" canonical >>= fun () ->
	  File_system.unlink canonical
	end
      else
	Lwt.return ()
    in
    Lwt_list.iter_s maybe_remove existing
end

let get_last_tlog tlog_dir =
  Logger.debug_ "get_last_tlog" >>= fun () ->
  get_tlog_names tlog_dir >>= fun tlog_names ->
  let new_c = get_count tlog_names in
  Logger.debug_f_ "new_c:%i" new_c >>= fun () ->
  Lwt.return (new_c, Filename.concat tlog_dir (file_name new_c))

let maybe_correct tlog_dir new_c last index node_id =
  if new_c > 0 && last = None
  then
    begin
     (* somebody sabotaged us:
	(s)he deleted the .tlog file but we have .tlf's
	meaning rotation happened correctly.
	We have to take counter measures:
	uncompress the .tlf into .tlog and remove it.
     *)
      let pc = new_c - 1 in
      let tlc_name = Filename.concat tlog_dir (archive_name pc) in
      let tlu_name = Filename.concat tlog_dir (file_name pc)  in
      Logger.warning_ "Sabotage!" >>= fun () ->
      Logger.info_f_ "Counter Sabotage: decompress %s into %s"
	    tlc_name tlu_name
      >>= fun () ->
      Compression.uncompress_tlog tlc_name tlu_name >>= fun () ->
      Logger.info_f_ "Counter Sabotage(2): rm %s " tlc_name >>= fun () ->
      File_system.unlink tlc_name >>= fun () ->
      Logger.info_ "Counter Sabotage finished" >>= fun () ->
      _validate_one tlu_name node_id ~check_marker:false >>= fun (last,new_index) ->
      Lwt.return (new_c -1 , last, new_index)

    end
  else
    Lwt.return (new_c, last, index)

let make_tlc2 tlog_dir use_compression fsync node_id =
  Logger.debug_f_ "make_tlc2 %S" tlog_dir >>= fun () ->
  get_last_tlog tlog_dir >>= fun (new_c, fn) ->
  _validate_one fn node_id ~check_marker:true >>= fun (last, index) ->
  maybe_correct tlog_dir new_c last index node_id >>= fun (new_c,last,new_index) ->
  Logger.debug_f_ "make_tlc2 after maybe_correct %s" (Index.to_string new_index) >>= fun () ->
  let msg =
    match last with
      | None -> "None"
      | Some e -> let i = Entry.i_of e in "Some" ^ (Sn.string_of i)
  in
  Logger.debug_f_ "post_validation: last_i=%s" msg >>= fun () ->
  let col = new tlc2 tlog_dir new_c last new_index use_compression node_id fsync in
  (* rewrite last entry with ANOTHER marker so we can see we got here *)
  begin
    match last with
      | None   -> Lwt.return ()
      | Some e ->
          Logger.debug_f_ "will write marker: new_c:%i \n%s" new_c (Tlogreader2.Index.to_string index)
          >>= fun() ->
             (* for some reason this will cause mayhem 'test_243' *)
          let i = Entry.i_of e in
          let v = Entry.v_of e in
          let marker = _make_open_marker node_id in
          _init_file tlog_dir new_c >>= fun file ->
          let oc = F.oc_of file in
          Tlogcommon.write_marker oc i v marker >>= fun () ->
          F.close file >>= fun () ->
          Logger.debug_f_ "wrote %S marker @i=%s for %S"
            (Log_extra.string_option2s marker) (Sn.string_of i) node_id  >>= fun () ->
          Lwt.return ()
  end
  >>= fun () ->
  Lwt.return col


let truncate_tlog filename =
  let skipper () entry = Lwt.return ()
  in
  let t =
    begin
      let folder,extension,index = folder_for filename None in
      if extension <> ".tlog"
      then Lwt.fail (Failure "Cannot truncate a compressed tlog")
      else
        begin
        let do_it ic =
          let lowerI = Sn.start
          and higherI = None
          and first = Sn.of_int 0
          in
          Lwt.catch
            (fun() -> folder ic ~index lowerI higherI ~first () skipper)
            (function
              | Tlogcommon.TLogCheckSumError pos ->
                let fd = Unix.openfile filename [ Unix.O_RDWR ] 0o600 in
                Lwt.return ( Unix.ftruncate fd (Int64.to_int pos) )
              | ex -> Lwt.fail ex
            ) >>= fun () ->
          Lwt.return 0
        in
        Lwt_io.with_file ~mode:Lwt_io.input filename do_it
        end
    end
  in Lwt_extra.run t


