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
type store_maker= ?read_only:bool -> string -> Store.store Lwt.t
open Tlogcommon

let collapse_until (tlog_coll:Tlogcollection.tlog_collection) 
    ((create_store:store_maker), copy_store, head_location) 
    (too_far_i:Sn.t) 
    (cb: Sn.t -> unit Lwt.t) =
  
  let new_location = head_location ^ ".clone" in
  Lwt_log.debug_f "Creating db clone at %s" new_location >>= fun () ->
  Lwt.catch (
    fun () ->
      copy_store head_location new_location true 
    ) (
     function 
      | Not_found -> Lwt_log.debug_f "head db at '%s' does not exist" head_location
      | e -> fail e
    )
  >>= fun () ->
  Lwt_log.debug_f "Creating store at %s" new_location >>= fun () ->
  create_store new_location >>= fun new_store ->
  Lwt.finalize (
    fun () ->
	  tlog_coll # get_infimum_i () >>= fun min_i ->
      let first_tlog = (Sn.to_int min_i) /  !Tlogcommon.tlogEntriesPerFile in 
	  let store_i = Store.consensus_i new_store in
	  let tfs = Sn.string_of too_far_i in
	  let tlog_entries_per_file = Sn.of_int (!Tlogcommon.tlogEntriesPerFile) in
	  let processed = ref 0 in
	  let start_i = 
		begin
		  match store_i with
		    | None -> Sn.start
		    | Some i -> Sn.succ i
		end
	  in
	  
	  begin  
		if start_i >= too_far_i 
		then
		  let msg = Printf.sprintf 
			"Store counter (%s) is ahead of end point (%s). Aborting"
			(Sn.string_of start_i) tfs in
		  Lwt.fail (Failure msg) 
		else
		  let acc = ref None in
		  let maybe_log =
		    begin
		      let lo = Sn.add start_i   (Sn.of_int 10) in
		      let hi = Sn.sub too_far_i (Sn.of_int 10) in
		      function 
		        | b when b < lo || b > hi ->  Lwt_log.debug_f "%s => store" (Sn.string_of b)
		        | b when b = lo -> Lwt_log.debug " ... => store"
		        | _ -> Lwt.return ()
		    end 
		  in
		  let add_to_store entry = 
            let i = Entry.i_of entry 
            and value = Entry.v_of entry 
            in
		    begin 
		      match !acc with
		        | None ->
		          begin
		            let () = acc := Some(i,value) in
		            Lwt_log.debug_f "update %s has no previous" (Sn.string_of i) 
		            >>= fun () ->
		            Lwt.return ()
		          end
		        | Some (pi,pv) ->
		          if pi < i then
		            begin
		              maybe_log pi >>= fun () ->
		              begin  
		                if Sn.rem pi tlog_entries_per_file = 0L 
		                then
		                  
		                  cb (Sn.of_int (first_tlog + !processed)) >>= fun () ->
		                Lwt.return( processed := !processed + 1 )
		                else
		                  Lwt.return ()
		              end 
		              >>= fun () ->
		              Store.safe_insert_value new_store pi pv >>= fun _ ->
		              let () = acc := Some(i,value) in
		              Lwt.return ()
		            end
		          else
		            begin
		              Lwt_log.debug_f "%s => skip" (Sn.string_of pi) >>= fun () ->
		              let () = acc := Some(i,value) in
		              Lwt.return ()
		            end
		    end
		  in
		  Lwt_log.debug_f "collapse_until: start_i=%s" (Sn.string_of start_i) 
		              >>= fun () ->
		  tlog_coll # iterate start_i too_far_i add_to_store 
		              >>= fun () ->
		  let m_si = Store.consensus_i new_store in
		  let si = 
		    begin
		      match m_si with
		        | None -> Sn.start
		        | Some i -> i
		    end
		  in
		  
		  Lwt_log.debug_f "Done replaying to head (%s : %s)" (Sn.string_of si) (Sn.string_of too_far_i) >>= fun() ->
		  begin
		    if si = Sn.pred (Sn.pred too_far_i) then
		      Lwt.return ()
		    else
		      let msg = Printf.sprintf "Head db has invalid counter: %s" (Sn.string_of si) in 
		      Lwt_log.debug msg >>= fun () ->
		      Store.close new_store >>= fun () ->
		      Lwt.fail (Failure msg)
		  end
		    
	  end
	  >>= fun () ->
	  Lwt_log.debug_f "Relocating store to %s" head_location >>= fun () ->
	  Store.relocate new_store head_location
  ) 
    ( 
      fun () -> Store.close new_store
    )

let _head_i (create_store:store_maker) head_location =
  Lwt.catch
    (fun () ->
      let read_only=true in
      create_store ~read_only head_location >>= fun head ->
      let head_io = Store.consensus_i head in
      Store.close head >>= fun () ->
      Lwt.return head_io
    )
    (fun exn -> 
      Lwt_log.info_f ~exn "returning assuming no I %S" head_location >>= fun () ->
      Lwt.return None
    )

let collapse_many tlog_coll 
    store_fs 
    tlogs_to_keep cb' cb =
  
  Lwt_log.debug_f "collapse_many" >>= fun () ->
  tlog_coll # get_tlog_count () >>= fun total_tlogs ->
  Lwt_log.debug_f "total_tlogs = %i; tlogs_to_keep=%i" total_tlogs tlogs_to_keep >>= fun () ->
  let ((create_store:store_maker),_,(head_location:string)) = store_fs in
  _head_i create_store head_location >>= fun head_io ->
  let last_i = tlog_coll # get_last_i () in
  Lwt_log.debug_f "head @ %s : last_i %s " (Log_extra.option2s Sn.string_of head_io) (Sn.string_of last_i)
  >>= fun () ->
  let head_i = match head_io with None -> Sn.start | Some i -> i in
  let lag = Sn.to_int (Sn.sub last_i head_i) in
  let npt = !Tlogcommon.tlogEntriesPerFile in
  let tlog_lag = (lag +npt - 1)/ npt in
  let tlogs_to_collapse = tlog_lag - tlogs_to_keep - 1 in
  Lwt_log.debug_f "tlog_lag = %i; tlogs_to_collapse = %i" tlog_lag tlogs_to_collapse >>= fun () ->
  if tlogs_to_collapse <= 0
  then
    Lwt_log.info_f "Nothing to collapse..." >>= fun () -> 
    cb' 0
  else
    begin
      Lwt_log.info_f "Going to collapse %d tlogs" tlogs_to_collapse >>= fun () ->
      cb' (tlogs_to_collapse+1) >>= fun () ->
      tlog_coll # get_infimum_i() >>= fun tlc_min ->
      let g_too_far_i = Sn.add (Sn.of_int 2) (Sn.add head_i (Sn.of_int (tlogs_to_collapse * npt))) in
      (* +2 because before X goes to the store, you need to have seen X+1 and thus too_far = X+2 *)
      Lwt_log.debug_f "g_too_far_i = %s" (Sn.string_of g_too_far_i) >>= fun () ->
      collapse_until tlog_coll store_fs g_too_far_i cb >>= fun () ->
      tlog_coll # remove_oldest_tlogs tlogs_to_collapse >>= fun () ->
      cb (Sn.div g_too_far_i (Sn.of_int !Tlogcommon.tlogEntriesPerFile)) 
    end
