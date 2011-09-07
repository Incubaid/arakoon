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


let collapse_until (tlog_coll:Tlogcollection.tlog_collection) (store:Store.store) (too_far_i:Sn.t) (cb: Sn.t -> unit Lwt.t) =
  let location = store # get_location() in
  store # clone () >>= fun new_store ->
  tlog_coll # get_infimum_i () >>= fun min_i ->
  let first_tlog = (Sn.to_int min_i) /  !Tlogcommon.tlogEntriesPerFile in 
  new_store # consensus_i () >>= fun store_i ->
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
      let add_to_store (i,update) = 
      begin 
        match !acc with
        | None ->
          begin
            let () = acc := Some(i,update) in
            Lwt_log.debug_f "update %s has no previous" (Sn.string_of i) 
            >>= fun () ->
            Lwt.return ()
          end
        | Some (pi,pu) ->
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
                Store.safe_insert_update new_store pi pu >>= fun _ ->
                let () = acc := Some(i,update) in
                Lwt.return ()
              end
          else
            begin
              Lwt_log.debug_f "%s => skip" (Sn.string_of pi) >>= fun () ->
              let () = acc := Some(i,update) in
              Lwt.return ()
            end
      end
      in
      Lwt_log.debug_f "collapse_until: start_i=%s" (Sn.string_of start_i) 
      >>= fun () ->
      tlog_coll # iterate start_i too_far_i add_to_store 
      >>= fun () ->
      new_store # consensus_i () >>= fun m_si ->
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
        new_store # close () >>= fun () ->
        Lwt.fail (Failure msg)
      end
      
    end
  >>= fun () ->
  Lwt_log.debug_f "Relocating store to %s" location >>= fun () ->
  new_store # relocate location true >>= fun () ->
  new_store # close ()

let collapse_many tlog_coll store tlogs_to_keep cb' cb =
  Lwt_log.debug_f "collapse_many" >>= fun () ->
  tlog_coll # get_tlog_count () >>= fun total_tlogs ->
  let tlogs_to_collapse = total_tlogs - tlogs_to_keep in
  Lwt_log.info_f "Going to collapse %d tlogs" tlogs_to_collapse >>= fun () ->
  cb' tlogs_to_collapse >>= fun () ->
  tlog_coll # get_infimum_i() >>= fun tlc_min ->
  let g_too_far_i = Sn.succ (Sn.add tlc_min (Sn.of_int (tlogs_to_collapse * !Tlogcommon.tlogEntriesPerFile))) in
  collapse_until tlog_coll store g_too_far_i cb >>= fun () ->
  tlog_coll # remove_oldest_tlogs tlogs_to_collapse 
  
