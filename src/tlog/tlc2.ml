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
open Tlog_map



let section = Logger.Section.main

let head_fname = "head.db"

let head_name () = head_fname

let to_tlog_name = Tlog_map.to_tlog_name
let to_archive_name = Tlog_map.to_archive_name

let folder_for = Tlog_map.folder_for


let _validate_list tlog_names node_id ~check_marker=
  Lwt_list.fold_left_s
    (fun _ tlog_name ->
      _validate_one tlog_name node_id ~check_marker ~check_sabotage:true)
    (None,None,0)
    tlog_names
  >>= fun (eo,index, pos) ->
  Logger.info_f_ "_validate_list %s => %s" (String.concat ";" tlog_names) (Index.to_string index) >>= fun () ->
  Lwt.return (TlogValidIncomplete, eo, index)


let validate_last tlog_map node_id ~check_marker=
  Logger.debug_f_ "validate_last ~check_marker:%b" check_marker >>= fun () ->
  TlogMap.get_tlog_names tlog_map >>= fun tlog_names ->
  match tlog_names with
    | [] -> _validate_list [] node_id ~check_marker
    | _ ->
      let n = List.length tlog_names in
      let last = List.nth tlog_names (n-1) in
      let fn = TlogMap.get_full_path tlog_map last in
      _validate_list [fn] node_id ~check_marker>>= fun r ->
      let (_validity, last_eo, _index) = r in
      match last_eo with
        | None ->
          begin
            if n > 1
            then
              let prev_last = List.nth tlog_names (n-2) in
              let last_non_empty = TlogMap.get_full_path tlog_map prev_last in
              _validate_list [last_non_empty] node_id ~check_marker
            else
              Lwt.return r
          end
        | Some _i -> Lwt.return r





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


let _init_file fsync_dir tlog_map =
  let c = TlogMap.get_tlog_number tlog_map in
  let fn = file_name c in
  let full_name = TlogMap.get_full_path tlog_map fn in
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
  Logger.debug_f_ "_init_file %s pos0:%Li" fn pos0 >>= fun () ->
  let f = F.make oc fd fn c pos0 in
  Lwt.return f


class tlc2
        ?cluster_id
        ?(compressor=Compression.Snappy)
        (head_dir:string)
        (last:Entry.t option) (index:Index.index) (file:F.t option)
        (tlog_map: TlogMap.t)
        (node_id:string)
        ~(should_fsync:Sn.t -> float -> bool)
        ~(fsync_tlog_dir:bool)
  =
  let _previous_entry = ref (Some last) in
  let set_previous_entry e =
    _previous_entry := Some e
  in
  let invalidate_previous_entry () =
    _previous_entry := None
  in
  let get_previous_entry () =
    match !_previous_entry with
    | None ->
       TlogMap.reinit tlog_map >>= fun last ->
       set_previous_entry last;
       Lwt.return last
    | Some p ->
       Lwt.return p
  in
  object(self: # tlog_collection)
    val mutable _file = file
    val mutable _index = index

    val mutable _compression_q = Lwt_buffer.create_fixed_capacity 5
    val mutable _compression_thread = None
    val mutable _compressing = false

    val mutable last_sync_time  = 0.0
    val mutable entries_since_last_sync = (Sn.pred Sn.start)

    val _closing = ref false
    val _write_lock = Lwt_mutex.create ()
    val _jc = (Lwt_condition.create () : unit Lwt_condition.t)
    initializer self # start_compression_loop ()


    method validate_last_tlog () =
      validate_last tlog_map node_id ~check_marker:false >>= fun r ->
      let (_validity, _entry, new_index) = r in
      let tlu = TlogMap.get_full_path tlog_map (TlogMap.get_tlog_number tlog_map|> file_name) in
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
        File_system.lwt_directory_list tlog_map.TlogMap.tlog_dir >>= fun entries ->
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

    method should_fsync now = should_fsync entries_since_last_sync (now -. last_sync_time)

    method log_value_explicit i value ~sync_override marker =
      Lwt_mutex.with_lock _write_lock
        (fun () ->
           begin
             self # _prelude i >>= fun (file, maybe_add_to_compression) ->
             let p = F.file_pos file in
             let oc = F.oc_of file in
             Tlogcommon.write_entry oc i value >>= fun total_size ->
             Lwt_io.flush oc >>= fun () ->
             begin
               let () = entries_since_last_sync <- Sn.succ entries_since_last_sync in
               let now = Unix.gettimeofday() in
               let time_since_last_sync = now -. last_sync_time in
               let should_fsync = sync_override || self # should_fsync time_since_last_sync in
               if should_fsync
               then
                 let () =
                   last_sync_time <- now;
                   entries_since_last_sync  <- Sn.zero
                 in
                 F.fsync file
               else
                 Lwt.return_unit
             end
             >>= fun () ->
             begin
               match maybe_add_to_compression with
               | None -> Lwt.return_unit
               | Some old_outer -> self # _add_compression_job old_outer
             end
             >>= fun ()->
             let entry = Entry.make i value p marker in
             set_previous_entry (Some entry);
             Index.note entry _index;
             let () = TlogMap.new_entry tlog_map total_size in
             Lwt.return total_size
           end)

    method log_value i value = self # log_value_explicit i value ~sync_override:false None

    method accept i v =
      let sync_override = Value.is_synced v in
      let marker = None in
      self # log_value_explicit i v ~sync_override marker

    method private _prelude i =
      match _file with
        | None ->
          begin
            let outer = TlogMap.outer_of_i tlog_map i in
            Logger.info_f_ "prelude i:%s: outer=%i No File" (Sn.string_of i) outer>>= fun () ->
            begin
              if outer <> 0 && (TlogMap.is_rollover_point tlog_map i)
              then
                (* the first thing logged falls exactly into a new tlog,
                   the compression job for the previous one(s) thus is not yet picked up *)
                let need_compression = TlogMap.old_uncompressed tlog_map in
                Lwt_list.iter_s (self # _add_compression_job) need_compression
              else
                Lwt.return ()
            end >>= fun () ->
            let () =
              if TlogMap.should_roll tlog_map
              then
                let _new_outer = TlogMap.new_outer tlog_map i in
                ()
            in
            _init_file fsync_tlog_dir tlog_map  >>= fun file ->
            _file <- Some file;
            _index <- Index.make (F.fn_of file);
            Lwt.return (file, None)
          end
        | Some (file:F.t) ->
          if TlogMap.should_roll tlog_map
          then
            let do_jump () =
              let old_outer = TlogMap.get_tlog_number tlog_map  in
              let new_outer = TlogMap.new_outer tlog_map i in
              Logger.info_f_ "should roll: i= %s old_outer = %i => jump to %i"
                             (Sn.string_of i)
                             old_outer
                             new_outer
              >>= fun () ->
              self # _jump_to_tlog file new_outer >>= fun file ->
              Lwt.return (file, Some old_outer)
            in
            do_jump ()
          else
            Lwt.return (file, None)

    method private _add_compression_job old_outer =
      let open Compression in
      match compressor with
      | Bz2 | Snappy ->
         begin
           let tlu = TlogMap.get_full_path tlog_map (file_name old_outer) in
           let tlc = TlogMap.get_full_path tlog_map (archive_name compressor old_outer) in
           let job = (tlu,tlc,compressor) in
           Logger.info_f_ "adding new task to compression queue %s,%s"
                          tlu tlc >>= fun () ->
           Lwt_buffer.add job _compression_q
         end
      | No -> Lwt.return ()

    method private _jump_to_tlog file new_outer =
      F.close file >>= fun () ->
      _init_file fsync_tlog_dir tlog_map >>= fun new_file ->
      _file <- Some new_file;
      let index =
        let fn = file_name new_outer in
        let full_name = TlogMap.get_full_path tlog_map fn in
        Index.make full_name
      in
      _index <- index;
      Lwt.return new_file



    method iterate (start_i:Sn.t) (too_far_i:Sn.t)
                   (f:Entry.t -> unit Lwt.t)
                   (cb : int -> unit Lwt.t)
      =
      let index = _index in
      Logger.debug_f_ "tlc2.iterate : index=%s" (Index.to_string index) >>= fun () ->
      TlogMap.iterate_tlog_dir tlog_map ~index start_i too_far_i
                               f cb


    method get_infimum_i () = TlogMap.infimum tlog_map

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
      S.make_store
        ~lcnum:Node_cfg.default_lcnum
        ~ncnum:Node_cfg.default_ncnum
        ?cluster_id
        head_name >>= fun store ->
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
      invalidate_previous_entry ();
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
          Llio.copy_stream ~length ~ic ~oc)
      >>= fun () ->
      self # _maybe_close_current_file ()
      >>= fun () ->
      let () = TlogMap.set_should_roll tlog_map in
      Logger.info_ "done: save_head"


    method invalidate () = invalidate_previous_entry ()

    method get_last_i () =
      get_previous_entry () >>= function
      | None ->
         Lwt.return Sn.start
      | Some pe ->
         let pi = Entry.i_of pe in
         Lwt.return pi

    method get_last_value i =
      get_previous_entry () >>= function
       | None -> Lwt.return None
       | Some pe ->
          let pi = Entry.i_of pe in
          if pi = i
          then Lwt.return (Some (Entry.v_of pe))
          else
            if i > pi
            then Lwt.return None
            else (* pi > i *)
              let msg = Printf.sprintf "get_last_value %s > %s can't look back so far"
                                       (Sn.string_of pi) (Sn.string_of i)
              in
              Lwt.fail_with msg

    method get_last () =
      get_previous_entry () >>= function
      | None -> Lwt.return None
      | Some pe -> Lwt.return (Some (Entry.v_of pe, Entry.i_of pe))

    method tlogs_to_collapse ~head_i ~last_i ~tlogs_to_keep =
      TlogMap.tlogs_to_collapse tlog_map head_i last_i tlogs_to_keep

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
      self # get_last () >>= function
      | None -> Logger.debug_f_ "... no last, we never logged anything=> no marker"
      | Some(v,i) ->
         begin
           match _file with
           | Some file ->
              let oc = F.oc_of file in
              let marker = Some (_make_close_marker node_id) in
              Tlogcommon.write_marker oc i v marker >>= fun () ->
              Logger.debug_f_ "wrote %S marker @i=%s for %S"
                              (Log_extra.string_option2s marker) (Sn.string_of i) node_id
              >>= fun () ->
              F.fsync file >>= fun () ->
              F.close file
           | None -> Logger.fatal_f_ "how can this be?"
         end


    method get_tlog_from_i (i:Sn.t) : int = TlogMap.outer_of_i tlog_map i

    method get_start_i (n:int) : Sn.t option = TlogMap.get_start_i tlog_map n
    method is_rollover_point i = TlogMap.is_rollover_point tlog_map i
    method next_rollover i     = TlogMap.next_rollover tlog_map i

    method get_tlog_count () =
      TlogMap.get_tlog_names tlog_map >>= fun tlogs ->
      Lwt.return (List.length tlogs)

    method which_tlog_file (n:int) =
      TlogMap.which_tlog_file tlog_map n



    method dump_tlog_file n oc =
      self # which_tlog_file n >>= fun co ->
      let canonical = match co with
        | Some fn -> fn
        | None ->
                  let fn = file_name n in
                  TlogMap.get_full_path tlog_map fn
      in
      Logger.debug_f_ "n = %03i => canonical=%s" n canonical >>= fun () ->
      let name = Filename.basename canonical in
      let extension = extension_of name in
      let start_i = TlogMap.get_start_i tlog_map n in
      match start_i with
      | None -> failwith (Printf.sprintf "tlogcollection doesn't know tlog:%i" n)
      | Some start_i ->
         begin
           Llio.output_string oc extension >>= fun () ->
           Sn.output_sn oc start_i         >>= fun () ->
           File_system.stat canonical      >>= fun stats ->
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
         end

    method private _maybe_close_current_file () =
      match _file with
      | None -> Lwt.return ()
      | Some file ->
         _file <- None;
         F.close file >>= fun () ->
         Lwt.return_unit

    method save_tlog_file first_i extension length ic =
      invalidate_previous_entry ();
      (* what with rotation (jump to new tlog), open streams, ...*)
      self # _maybe_close_current_file ()
      >>= fun () ->
      let new_outer = TlogMap.new_outer tlog_map first_i in
      let new_name = Printf.sprintf "%03i%s" new_outer extension in
      let canon = TlogMap.get_full_path tlog_map new_name in
      let tmp = canon ^ ".tmp" in

      Logger.info_f_
        "save_tlog_file: first_i:%s extension:%s in %s"
        (Sn.string_of first_i) extension tmp
      >>= fun () ->

      Lwt_io.with_file ~mode:Lwt_io.output tmp
                       (fun oc -> Llio.copy_stream ~length ~ic ~oc)
      >>= fun () ->
      File_system.rename tmp canon >>= fun () ->
      let () = TlogMap.set_should_roll tlog_map in
      Lwt.return_unit

    method remove_below i = TlogMap.remove_below tlog_map i
    method complete_file_to_deliver i = TlogMap.complete_file_to_deliver tlog_map i
  end



let make_tlc2 ?cluster_id
              ~compressor
              ?tlog_max_entries ?tlog_max_size
              ?(check_marker=true)
              tlog_dir tlf_dir
              head_dir
              ~(should_fsync: Sn.t -> float -> bool)
              node_id ~fsync_tlog_dir
  =
  Logger.debug_f_ "make_tlc2 %S" tlog_dir >>= fun () ->
  TlogMap.make ?tlog_max_entries ?tlog_max_size
               tlog_dir tlf_dir node_id ~check_marker
  >>= fun (tlog_map , last, index )->
  Logger.debug_f_ "make_tlc2 index = %s" (Index.to_string index)
  >>= fun () ->
  let msg =
    match last with
      | None -> "None"
      | Some e -> let i = Entry.i_of e in "Some " ^ (Sn.string_of i)
  in
  Logger.debug_f_ "post_validation: last_i=%s" msg >>= fun () ->
  (* maybe rewrite last entry with ANOTHER marker so we can see we got here *)
  begin
    match last with
      | None   -> Lwt.return None
      | Some e ->
         (* for some reason this will cause mayhem 'test_243' *)
         let i = Entry.i_of e in
         let v = Entry.v_of e in
         let marker = Some (_make_open_marker node_id) in
         _init_file fsync_tlog_dir tlog_map >>= fun file ->
         let oc = F.oc_of file in
         begin
           if check_marker
           then
             Tlogcommon.write_marker oc i v marker
             >>= fun () ->
             Logger.info_f_ "wrote %S marker @i=%s for %S"
                             (Log_extra.string_option2s marker)
                             (Sn.string_of i)
                             node_id
           else Lwt.return_unit
         end
         >>= fun () ->
         Lwt.return (Some file)
  end >>= fun file ->
  let col = new tlc2 head_dir last index
                file tlog_map
                ~compressor node_id ~should_fsync ~fsync_tlog_dir
                ?cluster_id
  in
  Lwt.return col

let _truncate_tlog filename =
  Logger.debug_f_ "_truncate %s" filename >>= fun () ->
  let skipper () _entry = Lwt.return () in
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
          )
      in
      Lwt_io.with_file ~mode:Lwt_io.input filename do_it
    end

let truncate_tlog filename =
  let t' =
    Lwt.catch
      (fun () ->
       _truncate_tlog filename
       >>= fun () ->
       Lwt.return 0)
      (fun ex ->
       Lwt_io.printlf "problem:%s%!" (Printexc.to_string ex) >>= fun () ->
       Lwt.return 1
      )
  in
  Lwt_main.run t'
