(*
Copyright 2016 iNuron NV
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

open Lwt.Infix
open Tlogcommon
open Tlogreader2

let section =
  let s= Logger.Section.make "tlog_map" in
  let () = Logger.Section.set_level s Logger.Debug in
  s

let tlog_regexp = Str.regexp "^[0-9]+\\.tlog$"
let file_regexp = Str.regexp "^[0-9]+\\.tl\\(og\\|f\\|c\\|x\\)$"

let extension_of filename =
  let len = String.length filename in
  let dot_pos = String.rindex filename '.' in
  String.sub filename dot_pos (len - dot_pos)

let extension comp =
  let open Compression in
  match comp with
    | Snappy -> ".tlx"
    | Bz2 -> ".tlf"
    | No -> ".tlog"

let to_archive_name comp fn =
  let length = String.length fn in
  let ext = String.sub fn (length -5) 5 in
  if ext = ".tlog"
  then
    let root = String.sub fn 0 (length -5) in
    let e = extension comp in
    root ^ e
  else failwith (Printf.sprintf "extension is '%s' and should be '.tlog'" ext)


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


let folder_for filename index =
  let extension = extension_of filename in
  match extension with
  | ".tlog" -> AU.fold, extension, index
  | ".tlx"  -> AS.fold, extension, None
  | ".tlf"  -> AC.fold, extension, None
  | ".tlc"  -> AO.fold, extension, None
  | _       -> failwith (Printf.sprintf "no folder for '%s'" extension)

let is_archive filename =
  let extension = extension_of filename in
  match extension with
  | ".tlx" | ".tlf" | ".tlc"-> true
  | _ -> false

let first_i_of filename tlx_dir =
  let read_uncompress ic inflate =
    Llio.input_string ic >>= fun compressed ->
    let uncompressed = inflate compressed in
    let buffer = Llio.make_buffer uncompressed 0 in
    let i =  Sn.sn_from buffer in
    Lwt.return i
  in
  let reader filename =
    match extension_of filename with
    | ".tlog" -> (fun ic -> Sn.input_sn ic )
    | ".tlx" ->
       (fun ic ->
         Compression.read_format ic Compression.Snappy >>= fun ()->
         Sn.input_sn ic >>= fun _last_i ->
         read_uncompress ic Compression.uncompress_snappy
       )
    | ".tlf"  ->
       (fun ic ->
         Compression.read_format ic Compression.Bz2 >>= fun ()->
         Sn.input_sn ic >>= fun _last_i ->
         read_uncompress ic Compression.uncompress_bz2
       )
    | ".tlc"  ->
       (fun ic ->
         Llio.input_int ic >>= fun _ ->
         read_uncompress ic Compression.uncompress_bz2
       )
    | x -> failwith (Printf.sprintf "not a tlog_file:%s" x)
  in
  let get_i filename =
    Lwt_io.with_file ~mode:Lwt_io.input filename (reader filename)
  in
  Lwt.catch
    (fun () -> get_i filename)
    (function
      | Unix.Unix_error(Unix.ENOENT, "open", _) when extension_of filename = ".tlog" ->
         (* the tlog might already be compressed in the mean time *)
         Lwt_log.info_f
           "Could not get first_i_of %s, trying compressed tlog now"
           filename >>= fun () ->
         let to_archive_name c f =
           Printf.sprintf "%s/%s" tlx_dir (to_archive_name c f |> Filename.basename)
         in
         Lwt.catch
           (fun () -> to_archive_name Compression.Snappy filename |> get_i)
           (function
             | Unix.Unix_error(Unix.ENOENT, _, _) -> to_archive_name Compression.Bz2 filename |> get_i
             | e -> Lwt.fail e
           )
      | exn ->
         Lwt_log.warning_f ~exn "Exception during first_i_of %s" filename >>= fun () ->
         Lwt.fail exn)
  >>= fun i ->
  Logger.debug_f_ "%s starts with i:%Li" filename i >>= fun () ->
  Lwt.return i


let get_number fn =
  let dot_pos = String.index fn '.' in
  let pre = String.sub fn 0 dot_pos in
  int_of_string pre

let archive_name comp c =
  let x = extension comp in
  Printf.sprintf "%03i%s" c x

let file_name c = Printf.sprintf "%03i.tlog" c

let _get_full_path tlog_dir tlx_dir name =
    if Filename.check_suffix name ".tlx"
       || Filename.check_suffix name ".tlx.part"
       || Filename.check_suffix name ".tlf"
       || Filename.check_suffix name ".tlf.part"
    then
      Filename.concat tlx_dir name
    else
      Filename.concat tlog_dir name

let get_last_tlog tlog_names =
  match List.length tlog_names with
    | 0 -> None
    | n -> let last = List.nth tlog_names (n-1) in
           Some last

let _get_entries dir invalid_dir_exn =
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
    )

let _get_tlog_names tlog_dir tlx_dir =
  _get_entries tlog_dir (Node_cfg.InvalidTlogDir tlog_dir)
  >>= fun tlog_entries ->
  (if tlog_dir = tlx_dir
   then
     Lwt.return []
   else
     _get_entries tlx_dir (Node_cfg.InvalidTlxDir tlx_dir))
  >>= fun tlf_entries ->
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
  Lwt.return sorted2

let _make_close_marker node_id = "closed:" ^ node_id
let _make_open_marker node_id =  "opened:" ^ node_id

type validation_result = (Entry.t option * Index.index * int)
exception TLCNotProperlyClosed of (string * string)

let _validate_one
      tlog_name node_id
      ~(check_marker:bool)
      ~(check_sabotage:bool)
    : validation_result Lwt.t
  =
  Logger.info_f_ "_validate_one %s" tlog_name >>= fun () ->
  let e2s e = let i = Entry.i_of e in Printf.sprintf "(%s,_)" (Sn.string_of i) in
  let prev_entry = ref None in
  let new_index = Index.make tlog_name in
  Lwt.catch
    (fun () ->
       let first = Sn.of_int 0 in
       let folder, _, index = folder_for tlog_name None in

       let do_it ic = folder ic ~index Sn.start None ~first None
                        (fun _a0 entry ->
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
                 let s = Some (_make_close_marker node_id) in
                 if Entry.check_marker e s
                 then !prev_entry
                 else if extension_of tlog_name = ".tlog"
                 then raise (TLCNotProperlyClosed (tlog_name, Entry.entry2s e))
                 else raise TLogSabotage
           in
           Lwt.return eo'
       end
       >>= fun eo' ->
       Logger.debug_f_ "XX:a=%s" (Log_extra.option2s e2s eo') >>= fun () ->
       Logger.debug_f_ "After validation index=%s" (Index.to_string new_index) >>= fun () ->
       File_system.stat tlog_name >>= fun stat ->
       let size = stat.Lwt_unix.st_size in
       Logger.debug_f_ "%s size=%i" tlog_name size >>= fun () ->
       Lwt.return (eo', new_index, size)
    )
    (function
     | Unix.Unix_error(Unix.ENOENT,_,_) ->
        let ext = extension_of tlog_name in
        let base = Filename.basename tlog_name in
        if (not check_sabotage) || (ext = ".tlog" && get_number base = 0)
        then Lwt.return (None, new_index, 0)
        else (*
         somebody sabotaged us (deleting the tlog file),
         or (more likely?) the previous incarnation died
         in catchup while it was downloading .tlx files.

         *)
          Logger.warning_f_ "not found: %s. %s was removed?!" tlog_name base >>= fun () ->
          Lwt.fail TLogSabotage
      | exn -> Lwt.fail exn
    )

module TlogMap = struct
  type tlog_number = int
  type is_archive = bool
  type item = { i : Sn.t ;
                n : tlog_number;
                is_archive: is_archive
              }
  type t = {
      tlog_dir: string;
      tlx_dir : string;
      tlog_max_entries : int;
      tlog_max_size : int;
      node_id : string;
      mutable tlog_size: int;
      mutable tlog_entries : int;
      mutable tlog_number: tlog_number;

      mutable i_to_tlog_number : item list; (* sorted: most recent first *)
      mutable should_roll: bool;
      lock: Lwt_mutex.t;
    }

  let show_map t =
    To_string.list
      (fun item ->
        Printf.sprintf "{ i:%s; n:%03i; is_archive: %b}"
                       (Sn.string_of item.i) item.n item.is_archive
      )
      t.i_to_tlog_number

  let new_entry t s =
    t.tlog_size <- t.tlog_size + s;
    t.tlog_entries <- t.tlog_entries + 1;
    if t.tlog_size >= t.tlog_max_size
       || t.tlog_entries = t.tlog_max_entries
    then
      let () = t.should_roll <- true in
      Logger.ign_info_f_ "should roll over: tlog_entries=%i" (t.tlog_entries)


  let get_tlog_number t= t.tlog_number

  let get_tlog_names t = _get_tlog_names t.tlog_dir t.tlx_dir

  let get_full_path t x = _get_full_path t.tlog_dir t.tlx_dir x


  let _init tlog_dir tlx_dir node_id tlog_max_entries tlog_max_size ~check_marker ~check_sabotage =
    let () = ignore (tlog_max_size, tlog_max_entries) in
    Logger.info_f_ "_init ~check_marker:%b ~check_sabotage:%b"
                   check_marker check_sabotage
    >>= fun () ->
    _get_tlog_names tlog_dir tlx_dir >>= fun tlog_names ->
    let tlog_name = match get_last_tlog tlog_names with
      | None -> file_name 0
      | Some n -> n
    in
    Logger.info_f_ "tlog_name:%s" tlog_name >>= fun () ->
    let full_path = _get_full_path tlog_dir tlx_dir tlog_name in
    _validate_one full_path node_id ~check_marker ~check_sabotage
    >>= fun (last, index, tlog_size) ->
    Lwt_list.fold_left_s
      (fun (acc,prev)  tlog_name ->
        let n = get_number tlog_name in
        let canonical = _get_full_path tlog_dir tlx_dir tlog_name in
        first_i_of canonical tlx_dir >>= fun i ->
        (if i >= prev
        then Lwt.return_unit
        else Lwt.fail_with
               (Printf.sprintf "%S starts with %s, which ain't higher than %s"
                               canonical
                               (Sn.string_of i) (Sn.string_of prev)
               )
        ) >>= fun () ->
        let is_archive = is_archive tlog_name in
        let acc' = {i;n;is_archive}:: acc in
        let prev' = i in
       Lwt.return (acc',prev')
      ) ([],-1L) tlog_names
    >>= fun (i_to_tlog_number0,_) ->
    let i_to_tlog_number =
      if i_to_tlog_number0 = []
      then [{i = 0L;n = 0;is_archive=false}]
      else i_to_tlog_number0
    in
    let last_i =
      match last with
      | None -> Sn.zero
      | Some e -> Entry.i_of e
    in

    let tlog_number, tlog_entries, should_roll =
      match i_to_tlog_number with
      | []                -> 0, 0, false
      | item :: _ when item.is_archive -> item.n, 0, true
      | item :: _ ->
         let tlog_entries = Sn.sub last_i item.i |> Sn.to_int in
         let should_roll = tlog_entries + 1 >= tlog_max_entries in
         item.n, tlog_entries, should_roll
    in
    Logger.info_f_ "_init: tlog_entries:%i should_roll:%b" tlog_entries should_roll
    >>= fun () ->
    Lwt.return ((tlog_entries, tlog_size, tlog_number, i_to_tlog_number, should_roll),
                last, index)

  let make ?tlog_max_entries
           ?tlog_max_size
           tlog_dir tlx_dir node_id
           ~check_marker
    =
    let with_default o d =
      match o with
      | None -> d
      | Some x -> x
    in
    let tlog_max_entries = with_default tlog_max_entries 100_000 in
    let tlog_max_size    = with_default tlog_max_size 32_000_000 in
    _init
      tlog_dir tlx_dir node_id ~check_marker ~check_sabotage:true
      tlog_max_entries tlog_max_size
    >>= fun ((tlog_entries, tlog_size,tlog_number,i_to_tlog_number, should_roll), last, index ) ->
    let lock = Lwt_mutex.create () in
    let t = {node_id; tlog_dir;tlx_dir;tlog_max_entries; tlog_max_size;
             tlog_entries; tlog_size; tlog_number; i_to_tlog_number;should_roll;
             lock;
            }
    in
    Lwt.return (t, last, index)

  let reinit t =
    Lwt_mutex.with_lock
      t.lock
      (fun () ->
        Logger.info_f_ "tlog_map.reinit" >>= fun () ->
        _init
          t.tlog_dir t.tlx_dir t.node_id ~check_marker:false ~check_sabotage:false
          t.tlog_max_entries t.tlog_max_size
        >>= fun ((_tlog_entries, tlog_size, tlog_number,i_to_tlog_number,should_roll), last,_) ->
        t.tlog_size   <- tlog_size;
        t.tlog_number <- tlog_number;
        t.i_to_tlog_number <- i_to_tlog_number;
        t.should_roll <- should_roll;
        Lwt.return last)

  let get_last_tlog t =
    Logger.debug_ "get_last_tlog" >>= fun () ->
    get_tlog_names t >>= fun tlog_names ->
    let new_c = match get_last_tlog tlog_names with
      | None -> file_name 0
      | Some n -> n
    in
    Logger.debug_f_ "new_c:%s" new_c >>= fun () ->
    let full_path = _get_full_path t.tlog_dir t.tlx_dir new_c in
    Lwt.return full_path


  let find_tlog_file t c =
    let open Compression in
    let f0 = archive_name Snappy c in
    let f1 = archive_name Bz2 c in
    let f2 = file_name c in
    let rec _find = function
      | [] -> Lwt.fail (Failure (Printf.sprintf "no tlog file for %3i" c))
      | f :: fs ->
         let full = _get_full_path t.tlog_dir t.tlx_dir f in
         File_system.exists full >>= fun ok ->
         if ok
         then Lwt.return full
         else _find fs
    in
    _find [f0;f1;f2]

  let fold_read tlog_map file_name
                ~index
                (lowerI:Sn.t)
                (too_far_i:Sn.t option)
                ~first
                (a0:'a)
                (f:'a -> Entry.t -> 'a Lwt.t) =

    let full_name = _get_full_path tlog_map.tlog_dir tlog_map.tlx_dir file_name in
    let folder, extension, index' = folder_for file_name index in
    Logger.debug_f_ "fold_read extension=%s => index':%s" extension
                    (Tlogreader2.Index.to_string index')
    >>= fun () ->
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
               find_tlog_file tlog_map c >>= fun full_an ->
               let real_folder,_,_ = folder_for full_an None in
               Logger.debug_f_ "folding compressed %s" full_an >>= fun () ->
               Lwt_io.with_file ~mode:Lwt_io.input full_an
                                (fun ic -> real_folder ic ~index:None lowerI too_far_i ~first a0 f)
             end
           else
             Lwt.fail exn
        | exn -> Logger.debug_f_ ~exn "filename:%s" full_name >>= fun () ->
                 Lwt.fail exn
      )

  let outer_of_i t i =
    let rec find = function
      | [] -> 0
      | item :: rest ->
         if  i >= item.i
         then item.n
         else find rest
    in
    find t.i_to_tlog_number

  let get_start_i t n =
    let rec find = function
      | [] -> None
      | item :: rest ->
         if item.n = n
         then Some item.i
         else find rest
    in
    let r = find t.i_to_tlog_number in
    (* Logger.ign_debug_f_ "get_start_i %s %i => %s"  (show_map t) n
                                                      (To_string.option Sn.string_of r) ;
     *)
    r



  let should_roll t = t.should_roll

  let set_should_roll t = t.should_roll <- true

  let new_outer t i =
    let n = t.tlog_number + 1 in
    let () = t.tlog_number <- n in
    let () = t.tlog_size <- 0 in
    let () = t.tlog_entries <- 0 in
    let () = t.should_roll <- false in
    let () = t.i_to_tlog_number <- {i; n;is_archive = false} :: t.i_to_tlog_number in
    n

  let infimum t =
    get_tlog_names t >>= fun names ->
    let v =
      match names with
      | [] -> Sn.zero
      | name :: _ ->
         let n = get_number name in
         match get_start_i t n with
         | None -> (* somebody added older tlog files behind our back ?!*)
            Sn.zero
         | Some i -> i
    in
    Logger.info_f_ "infimum=%s" (Sn.string_of v) >>= fun () ->
    Lwt.return v

  let which_tlog_file t n =
    let open Compression in
    let options = [archive_name Snappy n;
                   archive_name Bz2 n;
                   file_name n]
    in
    let canonicals = List.map (get_full_path t) options in
    let rec loop = function
      | []     -> Lwt.return None
      | x::xs  -> File_system.exists x >>=
                    (function
                     | true -> Lwt.return (Some x)
                     | false -> loop xs)
    in
    loop canonicals

  let  _find_start i items =
    let i' =
      let rec _start_of_tlog = function
        | [] -> Sn.zero
        | item :: _ when item.i <= i-> item.i
        | _ :: items -> _start_of_tlog items
      in
      _start_of_tlog items
    in
    let rec aux to_remove_rev = function
      | [] -> ([],[])
      | item :: rest when item.i >= i' ->
         (List.rev to_remove_rev), (item :: rest)
      | x :: rest -> aux (x::to_remove_rev) rest
    in
    aux [] (List.rev items)

  let remove_below t i =
    Lwt_mutex.with_lock
      t.lock
      (fun () ->
        Logger.debug_f_ "remove_below %s tlog_map:%s" (Sn.string_of i) (show_map t)
        >>= fun () ->
        let to_remove,to_keep_rev = _find_start i t.i_to_tlog_number in
        t.i_to_tlog_number <- List.rev to_keep_rev;

        let remove item  =
          which_tlog_file  t item.n >>= function
          | None -> Lwt.return_unit
          | Some canonical ->
             begin
               Logger.debug_f_ "Unlinking %s" canonical >>= fun () ->
               File_system.unlink canonical
             end
        in
        Lwt_list.iter_s remove to_remove >>= fun () ->
        Logger.debug_f_ "after remove_below %s tlog_map:%s" (Sn.string_of i) (show_map t) >>= fun () ->
        Lwt.return_unit
      )

  let is_rollover_point t i =
    let rec find = function
      | [] -> false
      | item :: _ when item.i = i -> true
      | item :: _ when item.i < i -> false
      | _ :: rest -> find rest
    in
    let r = find t.i_to_tlog_number in
    Logger.ign_debug_f_ "is_rollover_point %s => %b" (Sn.string_of i) r;
    r

  let complete_file_to_deliver t i =

    let rec find = function
      | [] -> None
      | item1 :: item0 :: _ when item0.i = i -> Some (item0.n,item1.i)
      | item  :: _ when item.i < i -> None
      | _ :: rest -> find rest
    in
    let r = find t.i_to_tlog_number in
    Logger.ign_debug_f_
      "complete_file_to_deliver %s : %s => %s"
      (Sn.string_of i) (show_map t)
      (To_string.option (fun (n,i) -> Printf.sprintf ("(%i,%Li)") n i) r);
    r

  let next_rollover t i =
    let rec find = function
      | [] -> None
      | item0 :: item1 :: _ when item0.i > i && i >= item1.i -> Some item0.i
      | _ :: rest -> find rest
    in
    let r = find t.i_to_tlog_number in
    (*let () =
      Logger.ign_debug_f_
        "next_rollover %s : %s => %s" (Sn.string_of i)
        (show_map t) (To_string.option Sn.string_of r)
    in *)
    r

  let iterate_tlog_dir t ~index start_i too_far_i f (cb: int -> unit Lwt.t) =
    let open Tlogcommon in
    let open Tlogreader2 in
    let tfs = Sn.string_of too_far_i in
    Logger.debug_f_ "TlogMap.iterate_tlog_dir start_i:%s too_far_i:%s ~index:%s"
                    (Sn.string_of start_i) tfs (Index.to_string index)
    >>= fun () ->
    get_tlog_names t >>= fun tlog_names -> (* TODO: we probably shouldn't look at the fs *)
    let acc_entry (_i0:Sn.t) entry =
      f entry >>= fun () -> let i = Entry.i_of entry in Lwt.return i
    in
    let num_tlogs = List.length tlog_names in
    let maybe_fold (cnt,low) fn =
      let fn_start = get_start_i t (get_number fn) in
      let low_n = Sn.succ low in
      match fn_start with
      | Some fn_start ->
         begin
           let fn_next  = next_rollover t fn_start in
           let should_fold =
             match fn_next with
             | None -> fn_start <= low_n
             | Some next -> fn_start <= low_n && low_n <= next
           in
           if should_fold
           then
             begin
               let first = fn_start in
               Logger.info_f_ "Replaying tlog file: %s [%s,...] (%d/%d)"
                              fn (Sn.string_of fn_start) cnt num_tlogs
               >>= fun () ->
               let t1 = Unix.gettimeofday () in
               fold_read t fn ~index low (Some too_far_i) ~first low acc_entry >>= fun low' ->
               Logger.info_f_ "Completed replay of %s, took %f seconds, %i to go"
                              fn (Unix.gettimeofday () -. t1) (num_tlogs - cnt)
               >>= fun () ->
               cb cnt >>= fun () ->
               Lwt.return (cnt+1, low')
             end
           else
             Lwt.return (cnt +1, low)
         end
      | None -> (* it's a file that's not in the map *)
         Lwt.return (cnt +1, low)
    in
    Lwt_list.fold_left_s maybe_fold (1,start_i) tlog_names
    >>= fun _x ->
    Lwt.return ()

  let tlogs_to_collapse t head_i last_i tlogs_to_keep =
    let () = Logger.ign_debug_f_
               "tlogs_to_collapse head_i:%s last_i:%s tlogs_to_keep:%i tlog_map:%s"
               (Sn.string_of head_i) (Sn.string_of last_i)
               tlogs_to_keep (show_map t)
    in

    let collapsable =
      List.filter (fun item -> item.i >= head_i) t.i_to_tlog_number
    in
    let len = List.length collapsable in
    let n_to_collapse = len - tlogs_to_keep  in
    let rec drop n = function
      | [] -> []
      | (_ :: rest ) as list ->
         if n = 1
         then list
         else drop (n-1) rest
    in
    let to_collapse_rev = drop tlogs_to_keep collapsable in
    let r = match to_collapse_rev with
    | [] -> None
    | item :: _ ->
       (*
          +2 because before X goes to the store,
          you need to have seen X+1 and thus too_far = X+2
       *)
       Some (n_to_collapse, Int64.add item.i 2L)
    in
    let () = Logger.ign_debug_f_
               "tlogs_to_collapse %s %s %i => %s"
               (Sn.string_of head_i)
               (Sn.string_of last_i)
               tlogs_to_keep
               (To_string.option (fun (n,i) -> Printf.sprintf "(%i,%s)" n (Sn.string_of i)) r)
    in
    r

  let old_uncompressed t =
    List.fold_left
      (fun acc item -> if item.is_archive then acc else item.n :: acc)
      []
      t.i_to_tlog_number |> List.rev

end
