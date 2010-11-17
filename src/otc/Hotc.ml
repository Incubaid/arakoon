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

open Otc
(* open Logging *)
open Lwt

module Hotc = struct

  type t = {
    filename:string;
    bdb:Bdb.bdb;
    mutex:Lwt_mutex.t;
  }


  let _do_locked t f =
    Lwt.catch (fun () -> Lwt_mutex.with_lock t.mutex f)
      (fun e -> Lwt.fail e)

  let create filename =
    let res = {
      filename = filename;
      bdb = Bdb._make ();
      mutex = Lwt_mutex.create ();
    } in
    _do_locked res
      (fun () ->
	let () = Bdb._dbopen res.bdb res.filename
	  (Bdb.oreader lor Bdb.owriter lor Bdb.ocreat lor Bdb.olcknb) in
	Lwt.return ()
      ) >>= fun () ->
    Lwt.return res

  let close t =
    Bdb._dbclose t.bdb

  let delete t =
    _do_locked t
      (fun () -> Bdb._dbclose t.bdb; Bdb._delete t.bdb; Lwt.return ())

  let _transaction t (f:Bdb.bdb -> 'a) =
    (* let _ = log "_transaction start" in *)
    let bdb = t.bdb in
    let _ = Bdb._tranbegin bdb in
    (* let _ = log "_transaction in" in *)
    let res = try
      let res = f bdb in
      let _ = Bdb._trancommit bdb in
  res
    with
      | Failure msg ->
    let _ = Bdb._tranabort bdb in
      (* let _ = log "_transaction aborted" in *)
      failwith msg
      | x ->
    let _ = Bdb._tranabort bdb in
      (* let _ = log "_transaction aborted" in *)
      raise x
    in
      (* let _ = log "_transaction done" in *)
      res

  let transaction t (f:Bdb.bdb -> 'a) =
    _do_locked t (fun () -> _transaction t f)


  let with_cursor bdb (f:Bdb.bdb -> 'a) =
    let cursor = Bdb._cur_make bdb in
    Lwt.finalize
      (fun () -> f bdb cursor)
      (fun () -> let () = Bdb._cur_delete cursor in Lwt.return ())

  let batch bdb (batch_size:int) (prefix:string) (start:string option) =
    (* Logging.log 
      "Hotc.batch %d prefix:'%s' start:%s" batch_size prefix 
      (Logging.show_string_option start); *)
    transaction bdb
      (fun db2 ->
	with_cursor db2
	  (fun db3 cur ->
	    let res = match start with
	      | None ->
		begin
		  try
		    let () = Bdb.jump db3 cur prefix in
		    None
		  with
		    | Not_found -> Some []
		end
	      | Some start2 ->
		let key = prefix ^ start2 in
		try
		  let () = Bdb.jump db3 cur key in
		  let key2 = Bdb.key db3 cur in
		  if key = key2 then
		    let () = Bdb.next db3 cur in
		    None
		  else None
		with
		  | Not_found -> Some []
	    in
	    match res with
              | Some empty -> Lwt.return empty
              | None ->
		let rec one build = function
		  | 0 -> Lwt.return (List.rev build)
		  | count ->
		    Lwt.catch (fun () ->
		      let key = Bdb.key db3 cur in
		      let prefix_len = String.length prefix in
		      let prefix2 = String.sub key 0 prefix_len in
		      let () = if prefix2 <> prefix then raise Not_found in
		      let value = Bdb.value db3 cur in
		      (* chop of prefix *)
		      let skey = String.sub key prefix_len ((String.length key) - prefix_len) in
		      (* let () = log "ZZZ k:'%s' v:'%s'" skey value in *)
		      Lwt.return (Some (skey,value))
		    ) (function | Not_found -> Lwt.return None | exn -> Lwt.fail exn) >>= function
		      | None -> Lwt.return (List.rev build)
		      | Some s ->
			Lwt.catch
			  (fun () ->
			    let () = Bdb.next db3 cur in
			    one (s::build) (count-1)
			  )
			  (function | Not_found -> Lwt.return (List.rev (s::build)) | exn -> Lwt.fail exn)
		in
		one [] batch_size
	  )
      )
end
