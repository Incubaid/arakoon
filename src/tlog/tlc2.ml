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

exception TLCCorrupt of (Int64.t * Sn.t)

let file_regexp = Str.regexp "^[0-9]+\\.tl\\(og\\|f\\|c\\)$"
let file_name c = Printf.sprintf "%03i.tlog" c 
let archive_extension = ".tlf"
let archive_name c = Printf.sprintf "%03i.tlf" c

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
  File_system.lwt_directory_list tlog_dir >>= fun entries ->
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
	  if get_number prev = get_number name (* x.tlf = x.tlog *)
	  then name :: rest (* skip prev: .tlf might not be ready yet *)
	  else name :: acc
    ) [] sorted 
  in
  let sorted2 = List.rev filtered2 in
  let log_e e = Lwt_log.debug_f "entry %s %i" e (get_number e) in
  Lwt_list.iter_s log_e sorted2 >>= fun () ->
  Lwt.return sorted2

let extension_of filename = 
  let lm = String.length filename - 1 in
  let dot_pos = String.rindex_from filename lm '.' in
  let len = lm - dot_pos +1 in
  String.sub filename dot_pos len

let folder_for filename = 
  let extension = extension_of filename in
  let r = 
    if extension = ".tlog" then Tlogreader2.AU.fold
    else if extension = archive_extension then Tlogreader2.C.fold
    else if extension = ".tlc" then Tlogreader2.O.fold
    else failwith (Printf.sprintf "no folder for '%s'" extension) 
  in
  r,extension

    
let fold_read tlog_dir file_name 
    (lowerI:Sn.t) 
    (too_far_i:Sn.t option) 
    ~first
    (a0:'a) 
    (f:'a -> Sn.t * Update.t -> 'a Lwt.t) =
  Lwt_log.debug "fold_read" >>= fun () ->
  let full_name = Filename.concat tlog_dir file_name in
  let folder, extension = folder_for file_name in 
  let ic_f ic = folder ic lowerI too_far_i ~first a0 f in
  Lwt.catch
    (fun () ->
      Lwt_io.with_file ~mode:Lwt_io.input full_name ic_f)
    (function 
      | Unix.Unix_error(_,"open",_) as exn ->
	if extension = ".tlog" 
	then (* file might have moved meanwhile *)
	  begin
	    Lwt_log.debug_f "%s became %s meanwhile " file_name archive_extension 
	    >>= fun () ->
	    let an = to_archive_name file_name in
	    let full_an = Filename.concat tlog_dir an in
	    Lwt_log.debug_f "folding compressed %s" an >>= fun () ->
	    Lwt_io.with_file ~mode:Lwt_io.input full_an 
	      (fun ic -> Tlogreader2.AC.fold ic lowerI too_far_i ~first a0 f)
	  end
	else
	  Lwt.fail exn
      | exn -> Lwt.fail exn)
	  
      

let _validate_one tlog_name =      
  Lwt_log.debug_f "Tlc2._validate_one %s" tlog_name >>= fun () ->
  let a2s (i,u) = Printf.sprintf "(%s,_)" (Sn.string_of i) in
  let prev_entry = ref None in 
  Lwt.catch
    (fun () ->
      let first = Sn.of_int 0 in
      let folder,_ = folder_for tlog_name in
      let do_it ic = folder ic Sn.start None ~first None
	(fun a0 (i,u) -> let r = Some (i,u) in let () = prev_entry := r in Lwt.return r)
      in
      Lwt_io.with_file tlog_name ~mode:Lwt_io.input do_it
      >>= fun a ->
      Lwt_log.debug_f "XX:a=%s" (Log_extra.option_to_string a2s a) 
      >>= fun () -> Lwt.return a
    )
    (function
      | Unix.Unix_error(Unix.ENOENT,_,_) -> Lwt.return None
      | Tlogcommon.TLogCheckSumError pos ->
        begin 
        match !prev_entry with 
        | Some(i,u) -> Lwt.fail (TLCCorrupt (pos,i))
        | None -> Lwt.fail (TLCCorrupt(pos, Sn.start))
        end
      | exn -> Lwt.fail exn
    )

let _validate_list tlog_names = 
  if tlog_names = [] 
  then Lwt.return (TlogValidIncomplete, None)
  else 
    begin
      Lwt_list.fold_left_s 
	(fun acc tn -> 
	  _validate_one tn 
          (* 
	     >>= fun eo ->
	     let e2s (i,u) = Printf.sprintf "i:%s" (Sn.string_of i) in
	     Lwt_log.debug_f "YY:%s" (Log_extra.option_to_string e2s eo)
	     >>= fun () ->
	     Lwt.return eo 
	  *)
	) None tlog_names
      >>= fun eo ->
      let r = match eo with
	| None -> (TlogValidIncomplete, None)
	| Some (i,u) ->
	  (TlogValidIncomplete,Some i)
      in Lwt.return r
    end
      
      
let validate_last tlog_dir = 
  Lwt_log.debug "validate_last" >>= fun () ->
  get_tlog_names tlog_dir >>= fun tlog_names ->
  match tlog_names with
    | [] -> _validate_list []
    | _ -> 
      let n = List.length tlog_names in
      let last = List.nth tlog_names (n-1) in
      let fn = Filename.concat tlog_dir last in
      _validate_list [fn] >>= fun (validity, last_i) ->
        match last_i with
          | None -> 
            begin
              if n > 1 then
                let prev_last = List.nth tlog_names (n-2) in 
                let last_non_empty = Filename.concat tlog_dir prev_last in
                _validate_list [last_non_empty]
              else
                Lwt.return (validity, last_i)
            end 
          | Some i -> Lwt.return (validity, last_i)


let validate tlog_dir = 
  _validate_list []


let get_count tlog_names = 
  match List.length tlog_names with
    | 0 -> 0
    | n -> let last = List.nth tlog_names (n-1) in
	   let length = String.length last in
	   let postfix = String.sub last (length -4) 4 in
	   let number = get_number last in
	   if postfix = "tlog" then 
	     number
	   else  number + 1


let _init_oc tlog_dir c =
  let fn = file_name c in
  let full_name = Filename.concat tlog_dir fn in
  Lwt_io.open_file ~flags:[Unix.O_CREAT;Unix.O_APPEND;Unix.O_WRONLY]
    ~perm:0o644
    ~mode: Lwt_io.output full_name

let iterate_tlog_dir tlog_dir start_i too_far_i f =
  let tfs = Sn.string_of too_far_i in
  Lwt_log.debug_f "Tlc2.iterate tlog_dir:%s start_i:%s too_far_i:%s" 
    tlog_dir (Sn.string_of start_i) tfs 
  >>= fun () ->
  get_tlog_names tlog_dir >>= fun tlog_names ->
  let acc_entry (i0:Sn.t) (i,u) = 
    Lwt_log.debug_f "doing: %s" (Sn.string_of i) >>= fun () ->
    f (i,u) >>= fun () -> Lwt.return i
  in
  let maybe_fold low fn =
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
    Lwt_log.debug_f "%s <?= lowp:%s &&  low:%s <? %s && (low:%s <?= %s) yields:%b" 
      (Sn.string_of test0 ) lowp_s 
      low_s (Sn.string_of test1) 
      low_s tfs
      test_result
    >>= fun () ->
    if test_result
    then 
      begin
	Lwt_log.debug_f "fold_read over: %s (test0=%s)" fn 
	  (Sn.string_of test0) >>= fun () ->
	let first = test0 in
	fold_read tlog_dir fn low (Some too_far_i) ~first low acc_entry
      end
    else Lwt.return low
  in
  Lwt_list.fold_left_s maybe_fold start_i tlog_names 
  >>= fun x ->
  Lwt.return ()    
    
class tlc2 (tlog_dir:string) (new_c:int) (last:(Sn.t * Update.t) option) (use_compression:bool) = 
  let inner = 
    match last with
      | None -> 0
      | Some (i,_) ->
	let step = (Sn.of_int !Tlogcommon.tlogEntriesPerFile) in
	(Sn.to_int (Sn.rem i step)) + 1
  in
object(self: # tlog_collection)
  val mutable _oc = None 
  val mutable _inner = inner (* ~ pos in file *)
  val mutable _outer = new_c (* ~ pos in dir *) 
  val mutable _previous_update = last


  method validate_last_tlog () = validate_last tlog_dir 

  method validate () = validate tlog_dir

  method log_update i update =
    self # _prelude i >>= fun oc ->
    Tlogcommon.write_entry oc i update >>= fun () -> 
    Lwt_io.flush oc >>= fun () ->
    let () = match _previous_update with
      | None -> _inner <- _inner +1
      | Some (pi,pu) -> if pi < i then _inner <- _inner +1 
    in
    _previous_update <- Some (i,update);
    Lwt.return ()
    
  method private _prelude i =
    match _oc with
      | None ->
	begin
	  let outer = Sn.div i (Sn.of_int !Tlogcommon.tlogEntriesPerFile) in
	  _outer <- Sn.to_int outer;
	  _init_oc tlog_dir _outer >>= fun oc ->
	  _oc <- Some oc;
	  Lwt.return oc
	end
      | Some oc -> 
	if i = Sn.mul (Sn.of_int (_outer + 1)) (Sn.of_int !Tlogcommon.tlogEntriesPerFile) 
	then
	  Lwt_log.debug_f "i= %s & outer = %i => rotate" (Sn.string_of i) _outer >>= fun () ->
	  self # _rotate oc 
	else
	  Lwt.return oc

      
  method private _rotate (oc:Lwt_io.output_channel) =
    Lwt_io.close oc >>= fun () ->
    _inner <- 0;
    let tlu = Filename.concat tlog_dir (file_name _outer) in
    let tlc = Filename.concat tlog_dir (archive_name _outer) in
    let tlc_temp = tlc ^ ".part" in
    let compress () =
      Lwt_log.debug_f "Compressing: %s into %s" tlu tlc_temp >>= fun () ->
      Compression.compress_tlog tlu tlc_temp  >>= fun () ->
      File_system.rename tlc_temp tlc >>= fun () ->
      Lwt_log.debug_f "unlink of %s" tlu >>= fun () ->
      Lwt.catch
	(fun () -> 
	  Lwt_unix.unlink tlu >>= fun () ->
	  Lwt.return ("ok: unlinked " ^ tlu)
	)
	(fun e -> Lwt.return (Printf.sprintf "warning: unlinking of %s failed" tlu))
      >>= fun msg ->
      Lwt_log.debug ("end of compress : " ^ msg) 
    in 
    begin
      if use_compression 
      then compress ()
      else Lwt.return ()
    end
    >>= fun () ->
    _outer <- _outer + 1;
    _init_oc tlog_dir _outer >>= fun new_oc ->
    _oc <- Some new_oc;
    Lwt.return new_oc



  method iterate (start_i:Sn.t) (too_far_i:Sn.t) (f:Sn.t * Update.t -> unit Lwt.t) =
    iterate_tlog_dir tlog_dir start_i too_far_i f


  method get_infimum_i () = 
    get_tlog_names tlog_dir >>= fun names ->
    let f = !Tlogcommon.tlogEntriesPerFile in
    let v = 
    match names with
      | [] -> Sn.of_int (-f)
      | h :: _ -> let n = Sn.of_int (get_number h) in
		  Sn.mul (Sn.of_int f) n
    in 
    Lwt_log.debug_f "Tlc2.infimum=%s" (Sn.string_of v) >>= fun () ->
    Lwt.return v

  method dump_head oc =
    let head_name = Filename.concat tlog_dir Tlogcollection.head_name in
    let stat = Unix.LargeFile.stat head_name in
    let length = stat.st_size in
    Lwt_log.debug_f "tlcs:: dump_head (filesize is %Li bytes)" length >>= fun ()->
    Llio.output_int64 oc length >>= fun () ->
    Lwt_io.with_file
      ~flags:[Unix.O_RDONLY]
      ~mode:Lwt_io.input
      head_name (fun ic -> Llio.copy_stream ~length ~ic ~oc)
    >>= fun () ->
    Lwt_io.flush oc >>= fun () ->
    Local_store.make_local_store head_name >>= fun head -> 
    head # consensus_i () >>= fun io ->
    head # close () >>= fun () ->
    Lwt_log.debug_f "head has consensus_i=%s" (Log_extra.option_to_string Sn.string_of io)
    >>= fun () ->
    let next_i = match io with
      | None -> Sn.start
      | Some i -> Sn.succ i
    in
    Lwt.return next_i

  method get_head_filename () = Filename.concat tlog_dir Tlogcollection.head_name 
    
  method save_head ic =
    Lwt_log.debug "save_head()" >>= fun () ->
    Llio.input_int64 ic >>= fun length ->
    let hf_name = self # get_head_filename () in
    Lwt_io.with_file 
      ~flags:[Unix.O_WRONLY;Unix.O_CREAT]
      ~mode:Lwt_io.output
      hf_name
      (fun oc -> Llio.copy_stream ~length ~ic ~oc )
    >>= fun () ->
    Lwt_log.debug "done: save_head"

  
 method get_last_i () =
    match _previous_update with
      | None -> Lwt.return Sn.start
      | Some (pi,pu) -> Lwt.return pi

  method get_last_update i =
    let vo =
      match _previous_update with
	| None -> None
	| Some (pi,pu) -> 
	  if pi = i then 
	    Some pu
	  else 
	    if i > pi then
	      None
	    else (* pi > i *)
	      let msg = Printf.sprintf "get_last_update %s > %s can't look back so far" 
		(Sn.string_of pi) (Sn.string_of i)
	      in
	      failwith msg
    in 
    Lwt.return vo
	    
  method close () =
    match _oc with
      | None -> Lwt.return ()
      | Some oc -> Lwt_io.close oc
end

let get_last_tlog tlog_dir =
  get_tlog_names tlog_dir >>= fun tlog_names ->
  let new_c = get_count tlog_names in
  Lwt_log.debug_f "new_c:%i" new_c >>= fun () ->
  Lwt.return (new_c, Filename.concat tlog_dir (file_name new_c))

let make_tlc2 tlog_dir use_compression =
  Lwt_log.debug "make_tlc" >>= fun () ->
  get_last_tlog tlog_dir >>= fun (new_c,fn) ->
  _validate_one fn >>= fun last ->
  let msg = 
    match last with
      | None -> "None"
      | Some (li,_) -> "Some" ^ (Sn.string_of li)
  in
  Lwt_log.debug_f "post_validation: last_i=%s" msg >>= fun () ->
  let col = new tlc2 tlog_dir new_c last use_compression in
  Lwt.return col


let truncate_tlog filename =
  let skipper () (i,u) =
    Lwt.return ()
  in
  let t =
    begin
      let folder,extension = folder_for filename in
      if extension <> ".tlog" then
          Lwt.fail (Failure "Cannot truncate a compressed tlog")
      else
        begin
        let do_it ic =
          let lowerI = Sn.start
          and higherI = None
          and first = Sn.of_int 0
          in
          Lwt.catch( fun() ->
             folder ic lowerI higherI ~first () skipper
          ) (
          function
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
  in Lwt_main.run t

let get_file_number i =
  let j = 
	  if i = Sn.start
	    then
	      Sn.start
	    else
	      Sn.pred i
  in
  Sn.div j (Sn.of_int !Tlogcommon.tlogEntriesPerFile)
  
