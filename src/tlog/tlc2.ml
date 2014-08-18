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

open Tlogcollection
open Tlogcommon
open Lwt
open Unix.LargeFile
open Lwt_buffer

open Tlogreader2

exception TLCNotProperlyClosed of string

let section = Logger.Section.main

let file_regexp = Str.regexp "^[0-9]+\\.tl\\(og\\|f\\|c\\|x\\)$"

let tlog_regexp = Str.regexp "^[0-9]+\\.tlog$"
let file_name c = Printf.sprintf "%03i.tlog" c

let extension comp =
  let open Compression in
  match comp with
    | Snappy -> ".tlx"
    | Bz2 -> ".tlf"
    | No -> ".tlog"

let archive_name comp c =
  let x = extension comp in
  Printf.sprintf "%03i%s" c x


let to_archive_name comp fn =
  let length = String.length fn in
  let ext = String.sub fn (length -5) 5 in
  if ext = ".tlog"
  then
    let root = String.sub fn 0 (length -5) in
    let e = extension comp in
    root ^ e
  else failwith (Printf.sprintf "extension is '%s' and should be '.tlog'" ext)



let head_fname = "head.db"

let get_full_path tlog_dir tlf_dir name =
  if Filename.check_suffix name ".tlx"
     || Filename.check_suffix name ".tlx.part"
     || Filename.check_suffix name ".tlf"
     || Filename.check_suffix name ".tlf.part"
  then
    Filename.concat tlf_dir name
  else
    Filename.concat tlog_dir name


let find_tlog_file tlog_dir tlf_dir c =
  let open Compression in
  let f0 = archive_name Snappy c in
  let f1 = archive_name Bz2 c in
  let f2 = file_name c in
  let rec _find = function
    | [] -> Lwt.fail (Failure (Printf.sprintf "no tlog file for %3i" c))
    | f :: fs ->
       let full = get_full_path tlog_dir tlf_dir f in
       File_system.exists full >>= fun ok ->
       if ok
       then Lwt.return full
       else _find fs
  in
  _find [f0;f1;f2]

let head_name () = head_fname

let get_file_number i = Sn.div i (Sn.of_int !Tlogcommon.tlogEntriesPerFile)
let get_tlog_i n = Sn.mul n (Sn.of_int !Tlogcommon.tlogEntriesPerFile)



let to_tlog_name fn =
  let length = String.length fn in
  let extension = String.sub fn (length -4) 4 in
  if extension = ".tlx"
     || extension = ".tlf"
     || extension = ".tlc"
  then
    let root = String.sub fn 0 (length -4) in
    root ^ ".tlog"
  else failwith (Printf.sprintf "to_tlog_name:extension is '%s' and should be one of .tlc .tlf .tlx" extension)

let get_number fn =
  let dot_pos = String.index fn '.' in
  let pre = String.sub fn 0 dot_pos in
  int_of_string pre

let extension_of filename =
  let len = String.length filename in
  let dot_pos = String.rindex filename '.' in
  String.sub filename dot_pos (len - dot_pos)

let get_tlog_names tlog_dir tlx_dir =
  let get_entries dir invalid_dir_exn =
    Lwt.catch
      (fun () ->
         File_system.lwt_directory_list dir
      )
      (function
        | Unix.Unix_error(Unix.EIO, fn, _) as exn ->
          if fn = "readdir"
          then Lwt.fail invalid_dir_exn
          else Lwt.fail exn
        | exn -> Lwt.fail exn
      ) in
  get_entries tlog_dir (Node_cfg.InvalidTlogDir tlog_dir) >>= fun tlog_entries ->
  (if tlog_dir = tlx_dir
   then
     Lwt.return []
   else
     get_entries tlx_dir (Node_cfg.InvalidTlxDir tlx_dir)) >>= fun tlf_entries ->
  let entries = List.rev_append tlf_entries tlog_entries in
  let filtered = List.filter
                   (fun e -> Str.string_match file_regexp e 0)
                   entries
  in
  let my_compare fn1 fn2 =
    let n1 = get_number fn1
    and n2 = get_number fn2 in
    if n1 = n2
       (* make .tlx < .tlf < .tlog , so we can filter later*)
    then
      let fn1x = extension_of fn1
      and fn2x = extension_of fn2
         in
         let order x = match x with
           | ".tlx"  -> 1
           | ".tlf"  -> 2
           | ".tlog" -> 3
           | _       -> 4
         in compare (order fn1x) (order fn2x)
    else compare n1 n2
  in
  let sorted = List.sort my_compare filtered in
  let filtered2 = List.fold_left
                    (fun acc name ->
                       match acc with
                         | [] -> name :: acc
                         | prev :: rest ->
                           if get_number prev = get_number name
                           then prev :: rest (* smaller is better *)
                           else name :: acc
                    ) [] sorted
  in
  let sorted2 = List.rev filtered2 in
  let log_e e = Logger.debug_f_ "entry %s %i" e (get_number e) in
  Lwt_list.iter_s log_e sorted2 >>= fun () ->
  Lwt.return sorted2


let folder_for filename index =
  let extension = extension_of filename in
  match extension with
  | ".tlog" -> Tlogreader2.AU.fold, extension, index
  | ".tlx"  -> Tlogreader2.AS.fold, extension, None
  | ".tlf"  -> Tlogreader2.AC.fold, extension, None
  | ".tlc"  -> Tlogreader2.AO.fold, extension, None
  | _       -> failwith (Printf.sprintf "no folder for '%s'" extension)

let fold_read tlog_dir tlf_dir file_name
      ~index
      (lowerI:Sn.t)
      (too_far_i:Sn.t option)
      ~first
      (a0:'a)
      (f:'a -> Entry.t -> 'a Lwt.t) =
  let full_name = get_full_path tlog_dir tlf_dir file_name in
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
            Logger.debug_f_ "%s became archive" file_name
            >>= fun () ->
            let c = get_number file_name in
            find_tlog_file tlog_dir tlf_dir c >>= fun full_an ->
            let real_folder,_,_ = folder_for full_an None in
            Logger.debug_f_ "folding compressed %s" full_an >>= fun () ->
            Lwt_io.with_file ~mode:Lwt_io.input full_an
              (fun ic -> real_folder ic ~index:None lowerI too_far_i ~first a0 f)
          end
        else
          Lwt.fail exn
      | exn -> Lwt.fail exn
    )

type validation_result = (Entry.t option * Checksum.Crc32.t option * Index.index)

let _make_close_marker node_id = "closed:" ^ node_id
let _make_open_marker node_id =  "opened:" ^ node_id

let _validate_one tlog_name node_id ~check_marker : validation_result Lwt.t =
  Logger.debug_f_ "Tlc2._validate_one %s" tlog_name >>= fun () ->
  let e2s e = let i = Entry.i_of e in Printf.sprintf "(%s,_)" (Sn.string_of i) in
  let prev_entry = ref None in
  let prev_checksum = ref None in
  let new_index = Index.make tlog_name in
  Lwt.catch
    (fun () ->
       let first = Sn.of_int 0 in
       let folder, _, index = folder_for tlog_name None in

       let do_it ic = folder ic ~index Sn.start None ~first (None, None)
                        (fun _ entry ->
                           let () = Index.note entry new_index in
                           let pcs = match !prev_entry with
                             | None -> None
                             | Some e ->
                               if Entry.i_of e < Entry.i_of entry
                               then
                                 let cs = Value.checksum_of (Entry.v_of e) in
                                 let () = prev_checksum := cs in
                                 cs
                               else
                                 !prev_checksum
                           in
                           let eo = Some entry in
                           let () = prev_entry := eo in
                           Lwt.return (eo, pcs))
       in
       Lwt_io.with_file tlog_name ~mode:Lwt_io.input do_it
       >>= fun (eo, pcs) ->
       begin
         if not check_marker
         then Lwt.return eo
         else
           let eo' =
             match eo with
               | None -> None
               | Some e ->
                 let s = Some (_make_close_marker node_id) in
                 if Entry.check_marker e s
                 then !prev_entry
                 else raise (TLCNotProperlyClosed (Entry.entry2s e))
           in
           Lwt.return eo'
       end
       >>= fun eo' ->
       Logger.debug_f_ "XX:a=%s" (Log_extra.option2s e2s eo') >>= fun () ->
       Logger.debug_f_ "After validation index=%s" (Index.to_string new_index) >>= fun () ->
       Lwt.return (eo', pcs, new_index)
    )
    (function
      | Unix.Unix_error(Unix.ENOENT,_,_) -> Lwt.return (None, None, new_index)
      | exn -> Lwt.fail exn
    )


let _validate_list tlog_names node_id ~check_marker=
  Lwt_list.fold_left_s (fun _ tn -> _validate_one tn node_id ~check_marker) (None, None, None) tlog_names >>= fun (eo, _, index) ->
  Logger.info_f_ "_validate_list %s => %s" (String.concat ";" tlog_names) (Index.to_string index) >>= fun () ->
  Lwt.return (TlogValidIncomplete, eo, index)




let validate_last tlog_dir tlf_dir node_id ~check_marker=
  Logger.debug_f_ "validate_last ~check_marker:%b" check_marker >>= fun () ->
  get_tlog_names tlog_dir tlf_dir >>= fun tlog_names ->
  match tlog_names with
    | [] -> _validate_list [] node_id ~check_marker
    | _ ->
      let n = List.length tlog_names in
      let last = List.nth tlog_names (n-1) in
      let fn = get_full_path tlog_dir tlf_dir last in
      _validate_list [fn] node_id ~check_marker>>= fun r ->
      let (_validity, last_eo, _index) = r in
      match last_eo with
        | None ->
          begin
            if n > 1
            then
              let prev_last = List.nth tlog_names (n-2) in
              let last_non_empty = get_full_path tlog_dir tlf_dir prev_last in
              _validate_list [last_non_empty] node_id ~check_marker
            else
              Lwt.return r
          end
        | Some _i -> Lwt.return r



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


let _init_file fsync_dir tlog_dir tlf_dir c =
  let fn = file_name c in
  let full_name = get_full_path tlog_dir tlf_dir fn in
  Lwt_unix.openfile full_name [Unix.O_CREAT;Unix.O_APPEND;Unix.O_WRONLY] 0o644 >>= fun fd ->
  begin
    if fsync_dir
    then
      File_system.fsync_dir_of_file full_name
    else
      Lwt.return ()
  end >>= fun () ->
  Lwt_unix.LargeFile.fstat fd >>= fun stats ->
  let pos0 = stats.st_size in
  let oc = Lwt_io.of_fd ~mode:Lwt_io.output fd in
  Logger.debug_f_ "_init_file pos0 : %Li" pos0 >>= fun () ->
  let f = F.make oc fd fn c pos0 in
  Lwt.return f


let iterate_tlog_dir tlog_dir tlf_dir ~index start_i too_far_i f =
  let tfs = Sn.string_of too_far_i in
  Logger.debug_f_ "Tlc2.iterate_tlog_dir tlog_dir:%s start_i:%s too_far_i:%s ~index:%s"
    tlog_dir (Sn.string_of start_i) tfs (Index.to_string index)
  >>= fun () ->
  get_tlog_names tlog_dir tlf_dir >>= fun tlog_names ->
  let acc_entry (_i0:Sn.t) entry = f entry >>= fun () -> let i = Entry.i_of entry in Lwt.return i in
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
        fold_read tlog_dir tlf_dir fn ~index low (Some too_far_i) ~first low acc_entry >>= fun x ->
        Logger.info_f_ "Completed replay of %s, took %f seconds, %i to go" fn (Sys.time () -. t1) (num_tlogs - cnt) >>= fun () ->
        Lwt.return (cnt+1,x)
      end
    else Lwt.return (cnt+1,low)
  in
  Lwt_list.fold_left_s maybe_fold (1,start_i) tlog_names
  >>= fun _x ->
  Lwt.return ()





class tlc2
        ?(compressor=Compression.Snappy)
        (tlog_dir:string) (tlf_dir:string) (head_dir:string) (new_c:int)
        (last:Entry.t option) (last_checksum:Checksum.Crc32.t option) (index:Index.index)
        (node_id:string) ~(fsync:bool) ~(fsync_tlog_dir:bool)
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
    val mutable _previous_checksum = last_checksum
    val mutable _compression_q = Lwt_buffer.create_fixed_capacity 5
    val mutable _compression_thread = None
    val mutable _compressing = false
    val _closing = ref false
    val _write_lock = Lwt_mutex.create ()
    val _jc = (Lwt_condition.create () : unit Lwt_condition.t)
    initializer self # start_compression_loop ()


    method validate_last_tlog () =
      validate_last tlog_dir tlf_dir node_id ~check_marker:false >>= fun r ->
      let (_validity, _entry, new_index) = r in
      let tlu = get_full_path tlog_dir tlf_dir (file_name _outer) in
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
      let compress_one tlu tlc compressor =
        File_system.exists tlc >>= fun tlc_exists ->
        if tlc_exists
        then
          begin
            Logger.info_f_ "Compression target %s already exists, this should be a complete .tlf" tlc >>= fun () ->
            File_system.unlink tlu
          end
        else
          begin
            Logger.info_f_ "Compressing: %s into %s" tlu tlc >>= fun () ->
            Compression.compress_tlog ~cancel:_closing tlu tlc compressor >>= fun () ->
            File_system.unlink tlu >>= fun () ->
            Logger.info_f_ "end of compress : %s -> %s" tlu tlc
          end
      in

      let rec loop () =
        Logger.info_ "Taking job from compression queue..." >>= fun () ->
        Lwt_buffer.take _compression_q >>= fun (tlu, tlc, compressor) ->
        let () = _compressing <- true in
        Lwt.catch
          (fun () -> compress_one tlu tlc compressor)
          (function
            | Canceled -> Lwt.fail Canceled
            | exn -> Logger.warning_ ~exn "exception inside compression, continuing anyway")
        >>= fun () ->
        Logger.info_ "Finished compression task, lets loop" >>= fun () ->
        let () = _compressing <- false in
        let () = Lwt_condition.signal _jc () in
        loop ()
      in

      let add_previous_compression_jobs () =
        File_system.lwt_directory_list tlog_dir >>= fun entries ->
        let filtered = List.filter
                         (fun e -> Str.string_match tlog_regexp e 0)
                         entries
        in

        (* Sort oldest-to-newest *)
        let sorted = List.sort (fun a b -> compare (get_number a) (get_number b)) filtered in

        let rec add_jobs = function
          | [] -> return ()
          | [_] -> return () (* Skip last *)
          | (hd :: tl) -> self # _add_compression_job (get_number hd) >>= fun () -> add_jobs tl
        in
        add_jobs sorted
      in
      Lwt.ignore_result (add_previous_compression_jobs ());

      let t =
        Lwt.catch
          loop
          (fun exn -> Logger.info_f_ ~exn "compression thread was terminated") in
      _compression_thread <- Some t;
      Lwt.ignore_result t


    method log_value_explicit i value ?(validate = true) sync marker =
      if validate && not (Value.validate self i value)
      then Lwt.fail (Value.ValueCheckSumError (i, value))
      else
        Lwt_mutex.with_lock _write_lock
          (fun () ->
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
                   let pi = Entry.i_of pe in
                   if pi < i
                   then
                     begin
                       _inner <- _inner + 1;
                       let pcs = Value.checksum_of (Entry.v_of pe) in
                       _previous_checksum <- pcs
                     end
               in
               let entry = Entry.make i value p marker in
               _previous_entry <- Some entry;
               Index.note entry _index;
               Lwt.return ()
             end)

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
            begin
              if (_outer <> 0) && (i = Sn.mul outer (Sn.of_int !Tlogcommon.tlogEntriesPerFile))
              then
                (* the first thing logged falls exactly into a new tlog,
                   the compression job for the previous one thus is not yet picked up *)
                self # _add_compression_job (pred _outer)
              else
                Lwt.return ()
            end >>= fun () ->
            _init_file fsync_tlog_dir tlog_dir tlf_dir _outer >>= fun file ->
            _file <- Some file;
            _index <- Index.make (F.fn_of file);
            Lwt.return file
          end
        | Some (file:F.t) ->
          if i >= (Sn.of_int !Tlogcommon.tlogEntriesPerFile) *: (Sn.of_int (_outer + 1))
          then
            let do_jump () =
              let new_outer = Sn.to_int (Sn.div i (Sn.of_int !Tlogcommon.tlogEntriesPerFile)) in
              Logger.info_f_ "i= %s & outer = %i => jump to %i" (Sn.string_of i) _outer new_outer
              >>= fun () ->
              self # _jump_to_tlog file new_outer
            in
            do_jump ()
          else
            Lwt.return file

    method private _add_compression_job old_outer =
      let open Compression in
      match compressor with
      | Bz2 | Snappy ->
         begin
           let tlu = get_full_path tlog_dir tlf_dir (file_name old_outer) in
           let tlc = get_full_path tlog_dir tlf_dir
                                   (archive_name compressor old_outer) in
           let job = (tlu,tlc,compressor) in
           Logger.info_f_ "adding new task to compression queue %s,%s"
                          tlu tlc >>= fun () ->
           Lwt_buffer.add job _compression_q
         end
      | No -> Lwt.return ()

    method private _jump_to_tlog file new_outer =
      F.close file >>= fun () ->
      let old_outer = _outer in
      _inner <- 0;
      _outer <- new_outer;
      _init_file fsync_tlog_dir tlog_dir tlf_dir new_outer >>= fun new_file ->
      _file <- Some new_file;
      let index =
        let fn = file_name _outer in
        let full_name = get_full_path tlog_dir tlf_dir fn in
        Index.make full_name
      in
      _index <- index;
      self # _add_compression_job old_outer >>= fun () ->
      Lwt.return new_file



    method iterate (start_i:Sn.t) (too_far_i:Sn.t) (f:Entry.t -> unit Lwt.t) =
      let index = _index in
      Logger.debug_f_ "tlc2.iterate : index=%s" (Index.to_string index) >>= fun () ->
      iterate_tlog_dir tlog_dir tlf_dir ~index start_i too_far_i f


    method get_infimum_i () =
      get_tlog_names tlog_dir tlf_dir >>= fun names ->
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
      Filename.concat head_dir (head_name())

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
      S.make_store ~lcnum:Node_cfg.default_lcnum
        ~ncnum:Node_cfg.default_ncnum head_name >>= fun store ->
      let io = S.consensus_i store in
      S.close ~flush:false ~sync:false store >>= fun () ->
      Logger.debug_f_ "head has consensus_i=%s" (Log_extra.option2s Sn.string_of io)
      >>= fun () ->
      let next_i = match io with
        | None -> Sn.start
        | Some i -> Sn.succ i
      in
      Lwt.return next_i

    method save_head ic =
      Logger.info_ "save_head()" >>= fun () ->
      Llio.input_int64 ic >>= fun length ->
      let hf_name = self # get_head_name () in
      let tmp = Printf.sprintf "%s.tmp" hf_name in
      File_system.unlink tmp >>= fun () ->
      Logger.info_f_ "Receiving %Li bytes into %S" length tmp >>= fun () ->

      File_system.with_tmp_file
        tmp
        hf_name
        (fun oc ->
          Llio.copy_stream ~length ~ic ~oc) >>= fun () ->

      Logger.info_ "done: save_head"


    method get_last_i () =
      match _previous_entry with
        | None -> Sn.start
        | Some pe -> Entry.i_of pe

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

    method get_previous_checksum i =
      match _previous_entry with
        | None -> _previous_checksum
        | Some pe ->
          let pi = Entry.i_of pe in
          if pi = i
          then _previous_checksum
          else
          if pi = Sn.pred i
          then Value.checksum_of (Entry.v_of pe)
          else
            let msg = Printf.sprintf "get_previous_checksum %s > %s can't look back so far"
                        (Sn.string_of pi) (Sn.string_of i)
            in
            failwith msg

    method set_previous_checksum cso =
      _previous_checksum <- cso

    method close ?(wait_for_compression=false) () =
      Lwt_mutex.lock _write_lock >>= fun () ->
      Logger.debug_ "tlc2::close()" >>= fun () ->
      begin
        if wait_for_compression
        then
          Logger.debug_ "waiting for compression to finish it's queue" >>= fun () ->
          Lwt_buffer.wait_until_empty _compression_q >>= fun () ->
          begin
            if _compressing
            then Lwt_condition.wait _jc
            else Lwt.return ()
          end
        else
          Lwt.return ()
      end >>= fun () ->
      begin
        _closing := true;
        match _compression_thread with
          | None -> assert false
          | Some t ->
            Logger.debug_f_ "Cancelling compression thread" >>= fun () ->
            (try
              Lwt.cancel t;
              Lwt.return ()
            with exn ->
              Logger.info_ ~exn "Exception while canceling compression thread")
      end >>= fun () ->
      Logger.debug_ "tlc2::closes () (part2)" >>= fun () ->
      let last_file () =
        match _file with
          | None -> _init_file fsync_tlog_dir tlog_dir tlf_dir _outer
          | Some file -> Lwt.return file
      in
      last_file () >>= fun file ->
      begin
        match self # get_last() with
          | None -> Lwt.return ()
          | Some(v,i) ->
            begin
              let oc = F.oc_of file in
              let marker = Some (_make_close_marker node_id) in
              Tlogcommon.write_marker oc i v marker >>= fun () ->
              Logger.debug_f_ "wrote %S marker @i=%s for %S"
                (Log_extra.string_option2s marker) (Sn.string_of i) node_id
            end
      end >>= fun () ->
      F.close file

    method get_tlog_from_i (i:Sn.t) = get_file_number i

    method get_tlog_count () =
      get_tlog_names tlog_dir tlf_dir >>= fun tlogs ->
      Lwt.return (List.length tlogs)

    method which_tlog_file (start_i : Sn.t) =
      let open Compression in
      let n = Sn.to_int (get_file_number start_i) in
      let an0 = archive_name Snappy n
      and an1 = archive_name Bz2 n
      and fn = file_name n
      in
      let an0_c = get_full_path tlog_dir tlf_dir an0 in
      let an1_c = get_full_path tlog_dir tlf_dir an1 in
      let fn_c = get_full_path tlog_dir tlf_dir fn
      in
      let rec loop = function
        | []     -> Lwt.return None
        | x::xs  -> File_system.exists x >>=
                      (function
                        | true -> Lwt.return (Some x)
                        | false -> loop xs)
      in
      loop [an0_c;an1_c;fn_c]


    method dump_tlog_file start_i oc =
      self # which_tlog_file start_i >>= fun co ->
      let canonical = match co with
        | Some fn -> fn
        | None -> let n = Sn.to_int (get_file_number start_i) in
                  let fn = file_name n in
                  get_full_path tlog_dir tlf_dir fn
      in
      Logger.debug_f_ "start_i = %Li => canonical=%s" start_i canonical >>= fun () ->
      let name = Filename.basename canonical in
      Llio.output_string oc name >>= fun () ->
      File_system.stat canonical >>= fun stats ->
      let length = Int64.of_int(stats.Unix.st_size)
      (* why don't they use largefile ? *)
      in
      Logger.info_f_ "dumping %S (%Li bytes)" canonical length >>= fun () ->
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

    method save_tlog_file ?(validate = true) name length ic =
      (* what with rotation (jump to new tlog), open streams, ...*)
      let canon = get_full_path tlog_dir tlf_dir name in
      let tmp = canon ^ ".tmp" in
      Logger.debug_f_ "save_tlog_file: %s" tmp >>= fun () ->
      let validate_and_copy oc =
        Sn.input_sn ic >>= fun i ->
        Llio.input_int32 ic >>= fun crc ->
        Llio.input_string ic >>= fun cmd ->
        let value = Value.value_from (Llio.make_buffer cmd 0) in
        if validate && not (Value.validate self i value)
        then Lwt.fail (Value.ValueCheckSumError (i, value))
        else
          Sn.output_sn oc i >>= fun () ->
          Llio.output_int32 oc crc >>= fun () ->
          Llio.output_string oc cmd >>= fun () ->
          let length = Int64.sub length (Lwt_io.position ic) in
          Llio.copy_stream ~length ~ic ~oc
      in
      Lwt_io.with_file ~mode:Lwt_io.output tmp validate_and_copy >>= fun () ->
      File_system.rename tmp canon


    method remove_oldest_tlogs count =
      get_tlog_names tlog_dir tlf_dir >>= fun existing ->
      let rec remove_one l = function
        | 0 -> Lwt.return ()
        | n ->
          let oldest = get_full_path tlog_dir tlf_dir (List.hd l) in
          Logger.debug_f_ "Unlinking %s" oldest >>= fun () ->
          File_system.unlink (oldest) >>= fun () ->
          remove_one (List.tl l) (n-1)
      in remove_one existing count

    method remove_below i =
      get_tlog_names tlog_dir tlf_dir >>= fun existing ->
      let maybe_remove fn =
        let n = get_number fn in
        let fn_stop = Sn.of_int ((n+1) * !Tlogcommon.tlogEntriesPerFile) in
        if fn_stop < i then
          begin
            let canonical = get_full_path tlog_dir tlf_dir fn in
            Logger.debug_f_ "Unlinking %s" canonical >>= fun () ->
            File_system.unlink canonical
          end
        else
          Lwt.return ()
      in
      Lwt_list.iter_s maybe_remove existing
  end

let get_last_tlog tlog_dir tlf_dir =
  Logger.debug_ "get_last_tlog" >>= fun () ->
  get_tlog_names tlog_dir tlf_dir >>= fun tlog_names ->
  let new_c = get_count tlog_names in
  Logger.debug_f_ "new_c:%i" new_c >>= fun () ->
  Lwt.return (new_c, get_full_path tlog_dir tlf_dir (file_name new_c))

let maybe_correct new_c last index =
  if new_c > 0 && last = None
  then
    begin
      (* somebody sabotaged us:
         (s)he deleted the .tlog file but we have .tlf's
         meaning rotation happened correctly.
         This means the marker can not be present.
         Let the node die; they should fix this manually.
      *)
      Lwt.fail TLogSabotage
    end
  else
    Lwt.return (new_c, last, index)

let make_tlc2 ~compressor tlog_dir tlf_dir head_dir ~fsync node_id ~fsync_tlog_dir =
  Logger.debug_f_ "make_tlc2 %S" tlog_dir >>= fun () ->
  get_last_tlog tlog_dir tlf_dir >>= fun (new_c, fn) ->
  _validate_one fn node_id ~check_marker:true >>= fun (last, previous_checksum, index) ->
  maybe_correct new_c last index >>= fun (new_c,last,new_index) ->
  Logger.debug_f_ "make_tlc2 after maybe_correct %s" (Index.to_string new_index) >>= fun () ->
  let msg =
    match last with
      | None -> "None"
      | Some e -> let i = Entry.i_of e in "Some" ^ (Sn.string_of i)
  in
  Logger.debug_f_ "post_validation: last_i=%s" msg >>= fun () ->
  let col = new tlc2 tlog_dir tlf_dir head_dir new_c last previous_checksum new_index ~compressor node_id ~fsync ~fsync_tlog_dir in
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
        let marker = Some (_make_open_marker node_id) in
        _init_file fsync_tlog_dir tlog_dir tlf_dir new_c >>= fun file ->
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
  let skipper () _entry = Lwt.return ()
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
                | Tlogcommon.TLogCheckSumError pos
                | Tlogcommon.TLogUnexpectedEndOfFile pos ->
                  let fd = Unix.openfile filename [ Unix.O_RDWR ] 0o600 in
                  Lwt.return ( Unix.ftruncate fd (Int64.to_int pos) )
                | ex -> Lwt.fail ex
              ) >>= fun () ->
            Lwt.return 0
          in
          Lwt_io.with_file ~mode:Lwt_io.input filename do_it
        end
    end
  in Lwt_main.run t
