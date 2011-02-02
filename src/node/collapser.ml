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

let collapse_until tlog_dir head_name too_far_i =
  let tfs = Sn.string_of too_far_i in
  Lwt_log.debug_f "collapse_until %s" tfs >>= fun () ->
  let cn_store = Filename.concat tlog_dir head_name in
  Local_store.make_local_store cn_store >>= fun store ->
  store # consensus_i () >>= fun store_i ->
  let start_i = 
    match store_i with
      | None -> Sn.start
      | Some i -> Sn.succ i
  in
  if start_i >= too_far_i 
  then
    let msg = Printf.sprintf "Store counter (%s) is ahead of end point (%s). Aborting"
      (Sn.string_of start_i) tfs in
    Lwt.fail (Failure msg) 
  else
    begin
      let acc = ref None in
      let add_to_store (i,update) = 
	match !acc with
	  | None ->
	    let () = acc := Some(i,update) in
	    Lwt_log.debug_f "update %s has no previous" (Sn.string_of i) >>= fun () ->
	    Lwt.return ()
	      | Some (pi,pu) ->
		if pi < i then
		  begin
		    Lwt_log.debug_f "%s => store" (Sn.string_of pi) >>= fun () ->
		    Store.safe_insert_update store pi pu >>= fun _ ->
		    let () = acc := Some(i,update) in
		    Lwt.return ()
		  end
		else
		  begin
		    Lwt_log.debug_f "%s => skip" (Sn.string_of pi) >>= fun () ->
		    let () = acc := Some(i,update) in
		    Lwt.return ()
		  end
      in
      Lwt_log.debug_f "collapse_until: start_i=%s" (Sn.string_of start_i) >>= fun () ->
      Tlc2.iterate_tlog_dir tlog_dir start_i too_far_i add_to_store >>= fun () ->
      store # close ()
    end

let copy_file fn1 fn2 = (* LOOKS LIKE Clone.copy_stream ... *)
  let bs = Lwt_io.default_buffer_size () in
  let buffer = String.create bs in
  let copy_all ic oc = 
    let rec loop () =
      Lwt_io.read_into ic buffer 0 bs >>= fun bytes_read ->
      if bytes_read > 0 
      then 
	begin
	  Lwt_io.write oc buffer >>= fun () -> loop ()
	end
      else
	Lwt.return ()    
    in
    loop () 
  in
  Lwt_io.with_file ~mode:Lwt_io.input fn1
    (fun ic ->
      Lwt_io.with_file ~mode:Lwt_io.output fn2 
	(fun oc ->copy_all ic oc)
    )

let mv_file source target = 
  Unix.rename source target; 
  Lwt.return ()

let unlink_file target = 
  Unix.unlink target; 
  Lwt.return ()

let collapse_many tlog_dir tlog_names head_name = 
  Lwt_log.debug_f "collapse_many" >>= fun () ->
  let new_name = head_name ^ ".next" in
  let cn1 = Filename.concat tlog_dir head_name 
  and cn2 = Filename.concat tlog_dir new_name 
  in
  Lwt.catch
    (fun () -> copy_file cn1 cn2)
    (function
      | Unix.Unix_error (Unix.ENOENT,x,y) -> 
	begin
	  Lwt_log.debug_f "%s,%s" x y >>= fun () ->
	  Lwt.return ()
	end
      | exn -> Lwt.fail exn)
  >>= fun () -> 
  let numbers = List.map Tlc2.get_number tlog_names in
  let sorted = List.sort (fun a b -> b - a ) numbers in
  let new_c = List.hd sorted in
  let border_i = 
    Sn.mul (Sn.of_int !Tlogcommon.tlogEntriesPerFile) (Sn.of_int (new_c+1)) in
  let future_i = Sn.add border_i (Sn.of_int 10) in
  Lwt_log.debug_f "collapse_many: entriesPerFile=%i;new_c=%i; future_i = %s" 
    !Tlogcommon.tlogEntriesPerFile
    new_c (Sn.string_of future_i) 
  >>= fun () ->
  collapse_until tlog_dir new_name future_i >>= fun () ->
  mv_file cn2 cn1 >>= fun () -> (* new head becomes effective head *)
  Lwt_list.iter_s
    (fun tlog_name -> 
      let cn = Filename.concat tlog_dir tlog_name in
      unlink_file cn )
    tlog_names
