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


open Lwt
open Tlogcommon

let section = Logger.Section.main

let uncompress_block compressed = 
  (* it exploded on the monkey when I did a Lwt_preemptive detach here *)
  let lc = String.length compressed in 
  let r = Bz2.uncompress compressed 0 lc in
  Lwt.return r

module Index = struct
  type index_r = { filename: string;
                   mutable mapping: (Sn.t * int64) list;
                 }
  type index = index_r option

  let make filename = Some {filename; mapping=[]}

  let replace filename = function
    | None -> None
    | (Some index_r) as idx -> 
      if filename = index_r.filename 
      then idx 
      else None

  let find_pos start_i = function
    | None -> 0L
    | Some index_r -> 
      let rec loop best = function
        | [] -> best
        | (i,p) :: rest ->
          if i <= start_i 
          then p
          else loop best rest
      in
      loop 0L index_r.mapping

  let match_filename fn index = 
    match index with 
      | None -> false
      | Some idxr -> idxr.filename = fn

  let note entry index= 
    let k = Sn.of_int 1000 in
    match index with
      | None      -> failwith "note None?"
      | Some idxr -> 
        let i = Entry.i_of entry in
        let pos = Entry.p_of entry in
        if (Sn.rem i k) = Sn.start 
        then idxr.mapping <- (i,pos) :: idxr.mapping 
          
  let to_string index = 
    match index with
      | None -> "None"
      | Some idxr ->
        let s = 
          String.concat "::"
            (List.map (fun (i,p) -> 
              Printf.sprintf "(%s,%Li)" (Sn.string_of i) p)
               idxr.mapping)
        in
        Printf.sprintf "Some {filename=%S;mapping=%s}" idxr.filename s
end

module type TR = sig
  val fold: Lwt_io.input_channel -> 
    index:Index.index ->
    Sn.t -> Sn.t option -> first:Sn.t ->
    'a ->
    ('a -> Entry.t -> 'a Lwt.t) -> 'a Lwt.t
    (** here this fold does not attempt to eliminate doubles:
	it makes the code simpler and any state-updates that you need
	to do this can be done in the acc anyway.
    **)
end

module U = struct

  let maybe_jump_forward ic index lowerI = 
    Logger.debug_f_ "maybe_fast_forward %s with %s" 
      (Sn.string_of lowerI) 
      (Index.to_string index) >>= fun () ->
    let pos = Index.find_pos lowerI index in
    if pos <> 0L 
    then 
      begin
        Logger.info_f_ "%s => jump to %Li" (Index.to_string index) pos >>= fun () ->
        Lwt_io.set_position ic pos
      end
    else Lwt.return () 
  

  let next ic =
    Lwt.catch
	  (fun () ->
	    Tlogcommon.read_entry ic >>= fun t ->
	    Lwt.return (Some t)
	  )
	  (function
	    | End_of_file -> (Lwt_io.close ic >>= fun () -> Lwt.return None )
	    | exn -> Lwt.fail exn)

  let rec skip_until ic lowerI =
    next ic >>= function
      | None -> Lwt.return None
      | (Some iu) as siu ->
        let i = Entry.i_of iu in
	    if i < lowerI
	    then skip_until ic lowerI
	    else Lwt.return siu
 
  let fold ic 
      ~index
      lowerI 
      (too_far_i:Sn.t option)
      ~first
      (a0:'a) (f:'a -> Entry.t -> 'a Lwt.t) =
    let () = ignore first in          
    let sno2s sno= Log_extra.option2s Sn.string_of sno in
    Logger.debug_f_ "U.fold %s %s ~index:%s" (Sn.string_of lowerI)
      (sno2s too_far_i) (Index.to_string index)
    >>= fun () ->

    let rec _fold (a:'a) iu =
      match too_far_i with
        | None -> 
	      begin 
            f a iu >>= fun a' -> 
            next ic >>= function
              | None -> Lwt.return a'
              | Some iu' -> _fold a' iu'
          end
        | Some hi ->
          let i = Entry.i_of iu in
          if (i >= hi) 
          then Lwt.return a
          else 
            begin
              f a iu >>= fun a' ->
              next ic >>= function
                | None -> Lwt.return a'
                | Some iu' -> _fold a' iu'
            end
    in 
    maybe_jump_forward ic index lowerI >>= fun () ->
    skip_until ic lowerI >>= function
      | None ->  Lwt.return a0
      | Some iu0 ->
        _fold a0 iu0
          
end


module C = struct
  let fold ic ~index (lowerI:Sn.t) (too_far_i:Sn.t option) ~first a0 f = 
    let () = ignore index in          
    Logger.debug_f_ "C.fold lowerI:%s too_far_i:%s ~first:%s" (Sn.string_of lowerI)
      (Log_extra.option2s Sn.string_of too_far_i) 
      (Sn.string_of first)
    >>= fun () ->
    let rec _skip_blocks () = 
      Sn.input_sn ic >>= fun last_i ->
      Llio.input_string ic >>= fun s ->
      Logger.debug_f_ "_skip_blocks:last_i=%s%!" (Sn.string_of last_i)
      >>= fun () ->
      if last_i < lowerI 
      then _skip_blocks ()
      else Lwt.return s
    in
    let _skip_in_block buffer pos =
      let beyond = String.length buffer in 
      let rec _loop (maybe_p:Entry.t option) pos =
	    if pos = beyond 
        then maybe_p, pos
	    else
	      begin
	        let entry1, pos1 = Tlogcommon.entry_from buffer pos in
            let i1 = Entry.i_of entry1 in
	        if i1 > lowerI 
	        then maybe_p, pos
	        else 
	          _loop (Some entry1) pos1
	      end
      in
      _loop None pos 
    in
    let _fold_block a buffer pos =
      Logger.debug_f_ "_fold_block:pos=%i" pos>>= fun() ->
      let rec _loop a p =
	    if p = (String.length buffer) 
	    then Lwt.return a
	    else
	      let buf_entry, pos2 = Tlogcommon.entry_from buffer p in
          begin 
            match too_far_i with 
              | None -> f a buf_entry
              | Some max_i ->
                let i_buf = Entry.i_of buf_entry in
                if i_buf >= max_i 
                then Lwt.return a
                else f a buf_entry
          end >>= fun a' ->
	      _loop a' pos2
      in
      _loop a pos
    in
    let maybe_read_buffer () =
      Lwt.catch 
	    (fun () -> Sn.input_sn ic >>= fun _ (* last i *) ->
	      Llio.input_string ic >>= fun compressed -> Lwt.return (Some compressed))
	    (function 
	      | End_of_file -> Lwt.return None
	      | e -> Lwt.fail e
	    )
    in
    let rec _fold_blocks a =
      Logger.debug_ "_fold_blocks " >>= fun () ->
      maybe_read_buffer () >>= function
	    | None -> Lwt.return a
	    | Some compressed ->
	      begin
	        Logger.debug_f_ "uncompressing: %i bytes" 
	          (String.length compressed) >>= fun () ->
	        uncompress_block compressed >>= fun buffer ->
	        Logger.debug_f_ "uncompressed size: %i bytes" 
	          (String.length buffer) >>= fun () ->
	        _fold_block a buffer 0 >>= fun a' ->
	        _fold_blocks a'
	      end
    in
    _skip_blocks () >>= fun compressed -> 
    Logger.debug_f_ "... to _skip_in_block %i" (String.length compressed) >>= fun () ->
    uncompress_block compressed >>= fun buffer ->
    Logger.debug_f_ "uncompressed (size=%i)" (String.length buffer) >>= fun () ->
    let maybe_first, pos = _skip_in_block buffer 0 in
    begin
      match maybe_first with
	    | None -> Lwt.return a0
	    | Some entry-> f a0 entry
    end >>= fun a' ->
    Logger.debug_ "post_skip_in_block" >>= fun () ->
    _fold_block (a':'a) buffer pos >>= fun a1 -> 
    Logger.debug_ "after_block" >>= fun () ->
    _fold_blocks a1
      
end

module O = struct (* correct but slow folder for .tlc (aka Old) format *)
  let fold ic ~index (lowerI:Sn.t) (too_far_i:Sn.t option) ~first a0 f = 
    let () = ignore index in          
    Logger.debug_f_ "O.fold lowerI:%s too_far_i:%s ~first:%s" (Sn.string_of lowerI)
      (Log_extra.option2s Sn.string_of too_far_i) 
      (Sn.string_of first)
    >>= fun () ->
    let _read_block () = 
      Llio.input_int ic >>= fun _n_entries ->
      Llio.input_string ic 
    in
    let _skip_in_block buffer pos =
      let beyond = String.length buffer in 
      let rec _loop (maybe_p:Entry.t option) pos =
	    if pos = beyond 
        then maybe_p, pos
	    else
	      begin
	        let entry1, pos1 = Tlogcommon.entry_from buffer pos in
            let i1 = Entry.i_of entry1 in
	        if i1 > lowerI 
	        then maybe_p, pos
	        else 
	          _loop (Some entry1) pos1
	      end
      in
      _loop None pos 
    in
    let _fold_block a buffer pos =
      Logger.debug_f_ "_fold_block:pos=%i" pos>>= fun() ->
      let rec _loop a p =
	    if p = (String.length buffer) 
	    then Lwt.return a
	    else
	      let buf_entry, pos2 = Tlogcommon.entry_from buffer p in
          begin 
            match too_far_i with 
              | None -> f a buf_entry
              | Some max_i ->
                let i_buf = Entry.i_of buf_entry in
                if i_buf >= max_i 
                then Lwt.return a
                else 
                  f a buf_entry
          end >>= fun a' ->
	      _loop a' pos2
      in
      _loop a pos
    in
    let maybe_read_buffer () =
      Lwt.catch 
	    (fun () -> Llio.input_int ic >>= fun _ (* n_entries *) ->
	      Llio.input_string ic >>= fun compressed -> Lwt.return (Some compressed)
        )
	    (function 
	      | End_of_file -> Lwt.return None
	      | e -> Lwt.fail e
	    )
    in
    let rec _fold_blocks a =
      Logger.debug_ "_fold_blocks " >>= fun () ->
      maybe_read_buffer () >>= function
	    | None -> Lwt.return a
	    | Some compressed ->
	      begin
	        Logger.debug_f_ "compressed: %i" (String.length compressed) >>= fun () ->
	        uncompress_block compressed >>= fun buffer ->
	        _fold_block a buffer 0 >>= fun a' ->
	        _fold_blocks a'
	      end
    in
    _read_block () >>= fun compressed -> 
    Logger.debug_f_ "... to _skip_in_block %i" (String.length compressed) >>= fun () ->
    uncompress_block compressed >>= fun buffer ->
    Logger.debug_f_ "uncompressed (size=%i)" (String.length buffer) >>= fun () ->
    let maybe_first, pos = _skip_in_block buffer 0 in
    begin
      match maybe_first with
	    | None -> Lwt.return a0
	    | Some entry-> f a0 entry
    end >>= fun a' ->
    Logger.debug_ "post_skip_in_block" >>= fun () ->
    _fold_block (a':'a) buffer pos >>= fun a1 -> 
    Logger.debug_ "after_block" >>= fun () ->
    _fold_blocks a1
      
end

module AU = (U: TR)
module AC = (C: TR)
module AO = (O: TR)
