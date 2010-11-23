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

let file_regexp = Str.regexp "[0-9]+\\.tl[og|c]"
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
  let pre = String.sub fn 0 3 in
  int_of_string pre

let get_tlog_names tlog_dir = 
  lwt_directory_list tlog_dir >>= fun entries ->
  let filtered = List.filter (fun e -> Str.string_match file_regexp e 0) entries in
  let sorted = List.sort compare filtered in
  Lwt_list.iter_s (fun e -> Lwt_log.debug_f "entry:%s" e) sorted >>= fun () ->
  Lwt.return sorted


    
let fold_read tlog_dir file_name 
    (lowerI:Sn.t) 
    (higherI:Sn.t option) 
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
  let ic_f ic = folder ic lowerI higherI a0 f in
  Lwt_io.with_file ~mode:Lwt_io.input full_name ic_f
      

let _validate_one tlog_name =      
  Lwt_log.debug_f "_validate_one %s" tlog_name >>= fun () ->
  let a2s (i,u) = Printf.sprintf "(%s,_)" (Sn.string_of i) in
  Lwt.catch
    (fun () ->
      let do_it ic = Tlogreader2.AU.fold ic Sn.start None None
	(fun a0 (i,u) ->  let r = Some (i,u) in Lwt.return r)
      in
      Lwt_io.with_file tlog_name ~mode:Lwt_io.input do_it
      >>= fun a ->
      Lwt_log.debug_f "XX:a=%s" (Log_extra.option_to_string a2s a) 
      >>= fun () -> Lwt.return a
    )
    (function
      | Unix.Unix_error(Unix.ENOENT,_,_) -> Lwt.return None
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
	let compress () =
	  Compression.compress_tlog tlu tlc  >>= fun () ->
	  Unix.unlink tlu;
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
      let lowp = Sn.succ low in
      let lowp_s = Sn.string_of lowp in
      let test_result = 
	(test0 <= lowp) &&
	(test1  > lowp) &&
	  lowp < consensus_i
      in
      Lwt_log.debug_f "%s <?= lowp:%s && %s >? lowp:%s && (lowp:%s <? %s) yields:%b" 
	(Sn.string_of test0 ) lowp_s 
	(Sn.string_of test1) lowp_s 
	lowp_s (Sn.string_of consensus_i)
	test_result
      >>= fun () ->
      if test_result
      then 
	begin
	  Lwt_log.debug_f "fold_read over: %s" fn >>= fun () ->
	  fold_read tlog_dir fn low (Some consensus_i) low acc_entry
	end
      else Lwt.return low
    in
    Lwt_list.fold_left_s maybe_fold lowerI tlog_names 
    >>= fun x ->
    Lwt.return ()

  method get_last_update i = 
    match _previous_update with
      | None -> Lwt.return None
      | Some (pi,pu) -> 
	if pi = i then 
	  Lwt.return (Some pu)
	else
	  Llio.lwt_failfmt "get_last_update %s<>%s" (Sn.string_of pi) (Sn.string_of i)
	    

  method close () =
    Lwt_io.close _oc
end
let make_tlc2 tlog_dir =
  Lwt_log.debug "make_tlc" >>= fun () ->
  get_tlog_names tlog_dir >>= fun tlog_names ->
  let new_c = get_count tlog_names in
  Lwt_log.debug_f "new_c:%i" new_c >>= fun () ->
  let fn = Filename.concat tlog_dir (file_name new_c) in
  _validate_one fn >>= fun last ->
  let msg = 
    match last with
      | None -> "None"
      | Some (li,_) -> "Some" ^ (Sn.string_of li)
  in
  Lwt_log.debug_f "post_validation: last_i=%s" msg >>= fun () ->
  let col = new tlc2 tlog_dir new_c last in
  Lwt.return col
