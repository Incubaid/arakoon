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

exception TLCCorrupt of (Int64.t * Sn.t)

let file_regexp = Str.regexp "^[0-9]+\\.tl\\(og\\|c\\)$"
let file_name c = Printf.sprintf "%03i.tlog" c 
let archive_name c = Printf.sprintf "%03i.tlc" c

let to_archive_name fn = 
  let length = String.length fn in
  let extension = String.sub fn (length -5) 5 in
  if extension = ".tlog" 
  then 
    let root = String.sub fn 0 (length -5) in
    root ^ ".tlc"
  else failwith (Printf.sprintf "extension is '%s' and should be '.tlog'" extension)


let to_tlog_name fn = 
  let length = String.length fn in
  let extension = String.sub fn (length -4) 4 in
  if extension = ".tlc" 
  then
    let root = String.sub fn 0 (length -4) in
    root ^ ".tlog"
  else failwith (Printf.sprintf "to_tlog_name:extension is '%s' and should be '.tlc'" extension)

let is_compressed fn = 
  let length = String.length fn in
  let extension = String.sub fn (length-4) 4 in
  extension = ".tlc"

let get_number fn =
  let dot_pos = String.index fn '.' in
  let pre = String.sub fn 0 dot_pos in
  int_of_string pre

let get_tlog_names tlog_dir = 
  lwt_directory_list tlog_dir >>= fun entries ->
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
	  if get_number prev = get_number name (* x.tlc = x.tlog *)
	  then name :: rest (* skip prev: .tlc might not be ready yet *)
	  else name :: acc
    ) [] sorted 
  in
  let sorted2 = List.rev filtered2 in
  let log_e e = Lwt_log.debug_f "entry %s %i" e (get_number e) in
  Lwt_list.iter_s log_e sorted2 >>= fun () ->
  Lwt.return sorted2


    
let fold_read tlog_dir file_name 
    (lowerI:Sn.t) 
    (higherI:Sn.t option) 
    ~first
    (a0:'a) 
    (f:'a -> Sn.t * Update.t -> 'a Lwt.t) =
  Lwt_log.debug "fold_read" >>= fun () ->
  let full_name = Filename.concat tlog_dir file_name in
  let folder, msg = 
    if is_compressed file_name then
      Tlogreader2.AC.fold, "compressed" 
    else
      Tlogreader2.AU.fold, "uncompressed"
  in
  Lwt_log.debug_f "%s: %s" msg file_name >>= fun () ->
  let ic_f ic = folder ic lowerI higherI ~first a0 f in
  Lwt.catch
    (fun () ->
      Lwt_io.with_file ~mode:Lwt_io.input full_name ic_f)
    (function 
      | Unix.Unix_error(_,"open",_) as exn ->
	if msg = "uncompressed" 
	then (* file might have moved meanwhile *)
	  begin
	    Lwt_log.debug_f "%s became .tlc meanwhile " file_name >>= fun () ->
	    let an = to_archive_name file_name in
	    let full_an = Filename.concat tlog_dir an in
	    Lwt_log.debug_f "folding compressed %s" an >>= fun () ->
	    Lwt_io.with_file ~mode:Lwt_io.input full_an 
	      (fun ic -> Tlogreader2.AC.fold ic lowerI higherI ~first a0 f)
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
      let do_it ic = Tlogreader2.AU.fold ic Sn.start None ~first None
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
      _validate_list [fn]


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
  let (oc:Lwt_io.output_channel) = 
    Lwt_io.open_file ~flags:[Unix.O_CREAT;Unix.O_APPEND;Unix.O_WRONLY]
      ~perm:0o644
      ~mode: Lwt_io.output full_name
  in oc
    
    
class tlc2 (tlog_dir:string) (new_c:int) (last:(Sn.t * Update.t) option)= 
  let inner = 
    match last with
      | None -> 0
      | Some (i,_) ->
	let step = (Sn.of_int !Tlogcommon.tlogEntriesPerFile) in
	Sn.to_int (Sn.rem i step)
  in
object(self: # tlog_collection)
  val mutable _oc = _init_oc tlog_dir new_c
  val mutable _inner = inner (* ~ pos in file *)
  val mutable _outer = new_c (* ~ pos in dir *)
  val mutable _previous_update = last


  method validate_last_tlog () = validate_last tlog_dir 

  method validate () = validate tlog_dir

  method log_update i update =
    Tlogcommon.write_entry _oc i update >>= fun () -> 
    Lwt_io.flush _oc >>= fun () ->
    let () = match _previous_update with
      | None -> _inner <- _inner +1
      | Some (pi,pu) -> if pi < i then _inner <- _inner +1 
    in
    _previous_update <- Some (i,update);
    self # _maybe_rotate () >>= fun () ->
    Lwt.return Tlogwriter.WRSuccess
    
  method private _maybe_rotate() =
    if _inner = !Tlogcommon.tlogEntriesPerFile then
      begin
	Lwt_io.close _oc >>= fun () ->
	_inner <- 0;
	let tlu = Filename.concat tlog_dir (file_name _outer) in
	let tlc = Filename.concat tlog_dir (archive_name _outer) in
	let tlc_temp = tlc ^ ".part" in
	let compress () =
	  Lwt_log.debug_f "Compressing: %s into %s" tlu tlc_temp >>= fun () ->
	  Compression.compress_tlog tlu tlc_temp  >>= fun () ->
	  Lwt_log.debug_f "Renaming: %s to %s" tlc_temp tlc >>= fun () ->
	  Unix.rename tlc_temp tlc;
	  Lwt_log.debug_f "unlink of %s" tlu >>= fun () ->
	  let msg = 
	    try
	      Unix.unlink tlu;
	      "ok: unlinked " ^ tlu
	    with _ -> Printf.sprintf "warning: unlinking of %s failed" tlu
	  in
	  Lwt_log.debug ("end of compress : " ^ msg) >>= fun () ->
	  Lwt.return () 
	in 
	Lwt_preemptive.detach (fun () -> Lwt.ignore_result (compress ())) () 
	>>= fun () ->
	_outer <- _outer + 1;
	_oc <- _init_oc tlog_dir _outer;
	Lwt.return ()
      end
    else
      begin
	Lwt.return ()
      end

  method iterate (start_i:Sn.t) (consensus_i:Sn.t) (f:Sn.t * Update.t -> unit Lwt.t) =
    Lwt_log.debug_f "tlc2::iterate start_i:%s consensus_i:%s" 
      (Sn.string_of start_i) (Sn.string_of consensus_i) >>= fun () ->
    let lowerI = start_i in
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
	  low <= consensus_i
      in
      Lwt_log.debug_f "%s <?= lowp:%s &&  low:%s <? %s && (low:%s <?= %s) yields:%b" 
	(Sn.string_of test0 ) lowp_s 
	low_s (Sn.string_of test1) 
	low_s (Sn.string_of consensus_i)
	test_result
      >>= fun () ->
      if test_result
      then 
	begin
	  Lwt_log.debug_f "fold_read over: %s (test0=%s)" fn 
	    (Sn.string_of test0) >>= fun () ->
	  let first = test0 in
	  fold_read tlog_dir fn low (Some consensus_i) ~first low acc_entry
	end
      else Lwt.return low
    in
    Lwt_list.fold_left_s maybe_fold lowerI tlog_names 
    >>= fun x ->
    Lwt.return ()

  method get_last_i () =
    match _previous_update with
      | None -> Lwt.return Sn.start
      | Some (pi,pu) -> Lwt.return pi

  method get_last_update i = 
    match _previous_update with
      | None -> Lwt.return None
      | Some (pi,pu) -> 
	if pi = i then 
	  Lwt.return (Some pu)
	else
	  Lwt_log.error_f "get_last_update %s<>%s" (Sn.string_of pi) (Sn.string_of i) >>= fun () ->
    Lwt.return (Some pu)
	    
  method close () =
    Lwt_io.close _oc
end

let get_last_tlog tlog_dir =
  get_tlog_names tlog_dir >>= fun tlog_names ->
  let new_c = get_count tlog_names in
  Lwt_log.debug_f "new_c:%i" new_c >>= fun () ->
  Lwt.return (new_c, Filename.concat tlog_dir (file_name new_c))

let make_tlc2 tlog_dir =
  Lwt_log.debug "make_tlc" >>= fun () ->
  get_last_tlog tlog_dir >>= fun (new_c,fn) ->
  _validate_one fn >>= fun last ->
  let msg = 
    match last with
      | None -> "None"
      | Some (li,_) -> "Some" ^ (Sn.string_of li)
  in
  Lwt_log.debug_f "post_validation: last_i=%s" msg >>= fun () ->
  let col = new tlc2 tlog_dir new_c last in
  Lwt.return col


let truncate_tlog filename =
  let skipper () (i,u) =
    Lwt.return ()
  in
  let t =
    begin
      if is_compressed filename then
          Lwt.fail (Failure "Cannot truncate a compressed tlog")
      else
        begin
        let folder = Tlogreader2.U.fold in
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

