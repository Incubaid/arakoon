open Lwt.Infix
open Tlogcommon
open Tlogreader2

let section = Logger.Section.main

let tlog_regexp = Str.regexp "^[0-9]+\\.tlog$"
let file_regexp = Str.regexp "^[0-9]+\\.tl\\(og\\|f\\|c\\|x\\)$"

let extension_of filename =
  let len = String.length filename in
  let dot_pos = String.rindex filename '.' in
  String.sub filename dot_pos (len - dot_pos)

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
  
let first_i_of filename =
  let extension = extension_of filename in
  let reader = 
    match extension with
    | ".tlog" -> (fun ic -> Sn.input_sn ic )                   
    | ".tlx" ->
       (fun ic ->
         Compression.read_format ic Compression.Snappy >>= fun ()->
         Sn.input_sn ic >>= fun _last_i ->
         Llio.input_string ic >>= fun compressed ->
         let uncompressed = Compression.uncompress_snappy compressed in
         let buffer = Llio.make_buffer uncompressed 0 in
         let i =  Sn.sn_from buffer in
         Lwt.return i
       )
    | ".tlf"  ->
       (fun ic ->
         Compression.read_format ic Compression.Bz2 >>= fun ()->
         Sn.input_sn ic >>= fun _last_i ->
         Llio.input_string ic >>= fun compressed ->
         let uncompressed = Compression.uncompress_bz2 compressed in
         let buffer = Llio.make_buffer uncompressed 0 in
         let i =  Sn.sn_from buffer in
         Lwt.return i)
    | ".tlc"  -> failwith "todo: first_i_if .tlc"
    | x -> failwith (Printf.sprintf "not a tlog_file:%s" x)
  in
  Lwt_io.with_file ~mode:Lwt_io.input filename reader >>= fun i ->
  Logger.debug_f_ "%s starts with i:%Li" filename i >>= fun () ->
  Lwt.return i
                   

let get_number fn =
  let dot_pos = String.index fn '.' in
  let pre = String.sub fn 0 dot_pos in
  int_of_string pre

let extension comp =
  let open Compression in
  match comp with
    | Snappy -> ".tlx"
    | Bz2 -> ".tlf"
    | No -> ".tlog"

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

let _validate_one tlog_name node_id ~check_marker : validation_result Lwt.t =
  Logger.debug_f_ "Tlog_map._validate_one %s" tlog_name >>= fun () ->
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
                 else raise (TLCNotProperlyClosed (tlog_name, Entry.entry2s e))
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
      | Unix.Unix_error(Unix.ENOENT,_,_) -> Lwt.return (None, new_index, 0)
      | exn -> Lwt.fail exn
    )

module TlogMap = struct
  type tlog_number = int
  type is_archive = bool
  type t = {
      tlog_dir: string;
      tlx_dir : string;
      tlog_max_entries : int;
      tlog_max_size : int;
      node_id : string;
      mutable tlog_size: int;
      mutable tlog_entries : int;
      mutable tlog_number: tlog_number;
      mutable i_to_tlog_number : (Sn.t * tlog_number * is_archive) list;
      mutable should_roll: bool;
    }
             
  let show_map t =
    "[" ^ 
    String.concat ";"
                  (List.map
                     (fun (i,n,a) ->
                       Printf.sprintf "(%s,%03i,%b)" (Sn.string_of i) n a
                     ) t.i_to_tlog_number
                  ) ^ "]"
                  
  let new_entry t s =
    t.tlog_size <- t.tlog_size + s;
    t.tlog_entries <- t.tlog_entries + 1;
    if t.tlog_size >= t.tlog_max_size
       || t.tlog_entries + 1 = t.tlog_max_entries
    then
      let () = t.should_roll <- true in
      Logger.ign_debug_f_ "should roll over"


  let get_tlog_number t= t.tlog_number

  let get_tlog_names t = _get_tlog_names t.tlog_dir t.tlx_dir

  let get_full_path t x = _get_full_path t.tlog_dir t.tlx_dir x


  let _init tlog_dir tlx_dir node_id ~check_marker =
    Logger.debug_f_ "_init ~check_marker:%b" check_marker >>= fun () ->
    _get_tlog_names tlog_dir tlx_dir >>= fun tlog_names ->
    let tlog_number = get_count tlog_names in
    Logger.debug_f_ "tlog_number:%i" tlog_number >>= fun () ->
    let full_path = _get_full_path tlog_dir tlx_dir (file_name tlog_number) in
    _validate_one full_path node_id ~check_marker
    >>= fun (last, index, tlog_size) ->
    Lwt_list.map_s
      (fun tlog_name ->
        let n = get_number tlog_name in
        let canonical = _get_full_path tlog_dir tlx_dir tlog_name in
        first_i_of canonical >>= fun i ->
        let b = is_archive tlog_name in
       Lwt.return (i,n, b)
      ) tlog_names
    >>= fun i_to_tlog_number ->
    let should_roll,last_start =
      let rec find = function
          | [] -> false , Sn.zero
          | [(i0,_,true)] -> true, i0
          | _ :: rest -> find rest
      in find i_to_tlog_number
    in
    let tlog_entries =
      match last with
      | None -> 0
      | Some e -> let i = Entry.i_of e in (Sn.to_int i) - (Sn.to_int last_start)
    in
    Lwt.return (tlog_entries, tlog_size, tlog_number, i_to_tlog_number, should_roll)
      
  let make ?tlog_max_entries
           ?tlog_max_size
           tlog_dir tlx_dir node_id
    =
    let with_default o d =
      match o with
      | None -> d
      | Some x -> x
    in
    let tlog_max_entries = with_default tlog_max_entries 100_000 in
    let tlog_max_size    = with_default tlog_max_size 32_000_000 in
    _init tlog_dir tlx_dir node_id ~check_marker:true
    >>= fun (tlog_entries, tlog_size,tlog_number,i_to_tlog_number, should_roll) ->
    
    let t = {node_id; tlog_dir;tlx_dir;tlog_max_entries; tlog_max_size;
             tlog_entries; tlog_size; tlog_number; i_to_tlog_number;should_roll;
            }
    in
    Lwt.return t

  let reinit t =
    _init t.tlog_dir t.tlx_dir t.node_id ~check_marker:false 
    >>= fun (tlog_entries, tlog_size, tlog_number,i_to_tlog_number,should_roll) ->
    t.tlog_size   <- tlog_size;
    t.tlog_number <- tlog_number;
    t.i_to_tlog_number <- i_to_tlog_number;
    t.should_roll <- should_roll;
    Lwt.return ()

  let get_last_tlog t =
    Logger.debug_ "get_last_tlog" >>= fun () ->
    get_tlog_names t >>= fun tlog_names ->
    let new_c = get_count tlog_names in
    Logger.debug_f_ "new_c:%i" new_c >>= fun () ->
    let full_path = _get_full_path t.tlog_dir t.tlx_dir (file_name new_c) in
    Lwt.return (new_c, full_path)

  
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
        | exn -> Lwt.fail exn
      )
      
  let outer_of_i t i =
    let rec find pn = function
      | [] -> pn
      | (i0,n0,_) :: rest ->
         if i0 < i
         then find n0 rest
         else n0
    in
    find 0 t.i_to_tlog_number
      
  let get_start_i t n =
    let rec find = function
      | [] -> Sn.zero (* TODO: maybe assert *)
      | (i0,n0,_) :: rest ->
         if n0 = n
         then i0 else
           find rest
    in
    let r = find t.i_to_tlog_number in
    Logger.ign_debug_f_ "get_start_i %s %i => %s"  (show_map t) n (Sn.string_of r) ;
    r

  
         
  let is_first_of_new t i = Sn.rem i (Sn.of_int t.tlog_max_entries) = Sn.zero

  let should_roll t = t.should_roll
 
  let new_outer t i =
    let r = t.tlog_number + 1 in
    let () = t.tlog_number <- r in
    let () = t.tlog_size <- 0 in
    let () = t.tlog_entries <- 0 in
    let () = t.should_roll <- false in
    r

  let infimum t =
    get_tlog_names t >>= fun names ->
    let f = t.tlog_max_entries in
    let v =
      match names with
      | [] -> Sn.of_int (-f)
      | h :: _ -> let n = Sn.of_int (get_number h) in
                  Sn.mul (Sn.of_int f) n
    in
    Logger.debug_f_ "Tlc2.infimum=%s" (Sn.string_of v) >>= fun () ->
    Lwt.return v

  let remove_below t i =
    get_tlog_names t >>= fun existing ->
    let maybe_remove fn =
      let n = get_number fn in
      let fn_stop = Sn.of_int ((n+1) * t.tlog_max_entries) in
      if fn_stop < i then
        begin
          let canonical = _get_full_path t.tlog_dir t.tlx_dir fn in
          Logger.debug_f_ "Unlinking %s" canonical >>= fun () ->
          File_system.unlink canonical
        end
      else
        Lwt.return ()
    in
    Lwt_list.iter_s maybe_remove existing

  let is_rollover_point t i =
    (* TODO: this gets called a zillion times in catchup...
     *)
    Logger.ign_debug_f_ "is_rollover_point %s" (Sn.string_of i); 
    let rec find = function
      | [] -> false
      | (i0,_,_ ) :: _ when i0 = i -> true
      | (i0,_,_ ) :: _ when i0 > i -> false
      | _ :: rest -> find rest
    in
    find t.i_to_tlog_number

  let complete_file_to_deliver t i =
    let rec find = function
      | [] -> None
      | (i0,n,_)  :: (i1,_,_) :: _ when i0 = i -> Some (n,i1)
      | (i0,_,_)  :: _ when i0 > i -> None
      | _ :: rest -> find rest
    in
    find t.i_to_tlog_number

  let next_rollover t i =
    let rec find = function
      | [] -> None
      | (i0,_,_) :: (i1,_,_) :: _ when i0 <= i && i < i1 -> Some i1
      | _ :: rest -> find rest
    in
    let r = find t.i_to_tlog_number in
    let () =
      Logger.ign_debug_f_
        "next_rollover %s : %s => %s" (Sn.string_of i)
        (show_map t) (To_string.option Sn.string_of r)
    in
    r

  let iterate_tlog_dir t ~index start_i too_far_i f =
    let open Tlogcommon in
    let open Tlogreader2 in
    let tfs = Sn.string_of too_far_i in
    Logger.debug_f_ "TlogMap.iterate_tlog_dir start_i:%s too_far_i:%s ~index:%s"
                    (Sn.string_of start_i) tfs (Index.to_string index)
    >>= fun () ->
    get_tlog_names t >>= fun tlog_names ->
    let acc_entry (_i0:Sn.t) entry =
      f entry >>= fun () -> let i = Entry.i_of entry in Lwt.return i
    in
    let num_tlogs = List.length tlog_names in
    let maybe_fold (cnt,low) fn =
      let fn_start = get_start_i t (get_number fn) in
      let fn_next  = next_rollover t fn_start in
      let low_n = Sn.succ low in
      (* 000.tlx starting from (1, 14001) *)
      let should_fold = 
        match fn_next with
        | None -> fn_start <= low_n
        | Some next -> fn_start <= low_n && low_n < next
      in
      Logger.debug_f_ "maybe_fold %s low:%s" fn (Sn.string_of low) >>= fun () ->
      Logger.debug_f_ "fn: fn_start:%s <= low_n:%s fn_next:%s => %b"
                      (Sn.string_of fn_start) (Sn.string_of low_n)
                      (To_string.option Sn.string_of fn_next)
                      should_fold
      >>= fun () ->
      
      let fold () =
        Logger.debug_f_ "fold over: %s: [%s,...) "
                           fn (Sn.string_of fn_start) 
           >>= fun () ->
           let first = fn_start in
           Logger.info_f_ "Replaying tlog file: %s (%d/%d)" fn cnt num_tlogs  >>= fun () ->
           let t1 = Unix.gettimeofday () in
           fold_read t fn ~index low (Some too_far_i) ~first low acc_entry >>= fun low' ->
           Logger.info_f_ "Completed replay of %s, took %f seconds, %i to go"
                          fn (Unix.gettimeofday () -. t1) (num_tlogs - cnt)
           >>= fun () ->
           Lwt.return (cnt+1, low')
      in
      if should_fold then fold () else Lwt.return (cnt+1,low)
    in
    Lwt_list.fold_left_s maybe_fold (1,start_i) tlog_names
    >>= fun _x ->
    Lwt.return ()
      
  let tlogs_to_collapse t head_i last_i tlogs_to_keep =
    let lag = Sn.to_int (Sn.sub last_i head_i) in
    let npt = t.tlog_max_entries in
    let tlog_lag = (lag +npt - 1)/ npt in
    let tlogs_to_collapse = tlog_lag - tlogs_to_keep - 1 in
    tlogs_to_collapse

end
