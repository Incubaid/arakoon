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

let collapse_one tlog_dir tlog_name head_name =
  let cn_store = Filename.concat tlog_dir head_name in
  Local_store.make_local_store cn_store >>= fun store ->
  let filename = Filename.concat tlog_dir tlog_name in
  let folder = if Tlc2.is_compressed filename 
    then Tlogreader2.C.fold
    else Tlogreader2.U.fold
  in
  (* TODO: FACTOR OUT this piece is VERY similar to what happens in catchup phase 2 *)
  let acc = ref None in
  let add_to_store () (i,update) = 
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
  (* TODO: FACTOR OUT this piece is VERY similar to dump-tlog setup *)
  let do_stream ic = 
    let lowerI = Sn.start
    and higherI = None
    and first = Sn.of_int 0 
    and a0 = () in
    folder ic lowerI higherI ~first a0 add_to_store >>= fun () ->
    Lwt.return ()
  in
  Lwt.finalize
    (fun () -> Lwt_io.with_file ~mode:Lwt_io.input filename do_stream)
    (fun () -> store # close ())
    
