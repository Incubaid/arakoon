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
open Lwt


class mem_tlog_collection tlog_dir use_compression =
object (self: #tlog_collection)

  val mutable data = []
  val mutable last_update = None

  method validate_last_tlog () =
    let io = match last_update with
      | None -> None
      | Some (i,u) -> Some i 
    in
    Lwt.return (TlogValidComplete, io)

  method get_infimum_i () = Lwt.return Sn.start

  method get_last_i () =
    match last_update with
    | None -> Lwt.return Sn.start
    | Some( i, u ) -> Lwt.return i

  method iterate i last_i f =
    let data' = List.filter (fun (ei,eu) -> ei >= i && ei <= last_i) data in
    Lwt_list.iter_s f (List.rev data')

  method get_tlog_count() = failwith "not supported"

  method dump_tlog_file start_i oc = failwith "not supported"

  method save_tlog_file name length ic = failwith "not supported"

  method  log_update i u ~sync=
    let () = data <- (i,u)::data in
    let () = last_update <- (Some (i,u)) in
    Lwt.return ()

  method get_last_update i =
    match last_update with
      | None ->
	begin
	  Lwt_log.info_f "get_value: no update logged yet" >>= fun () ->
	  Lwt.return None
	end
      | Some (i',x) ->
	begin
	  if i = i'
	  then Lwt.return (Some x)
	  else
	    (Lwt_log.info_f "get_value: i(%s) is not latest update" (Sn.string_of i) >>= fun () ->
	     Lwt.return None)
	end

  method dump_head oc = Llio.lwt_failfmt "not implemented"
  method save_head ic = Llio.lwt_failfmt "not implemented"

  method get_head_name () = failwith "not implemented"

  method get_tlog_from_name n = failwith "not implemented"
  
  method get_tlog_from_i = failwith "not implemented"   
  
  method close () = Lwt.return ()
  
  method remove_oldest_tlogs count = Lwt.return ()

  method remove_below i = Lwt.return ()
end

let make_mem_tlog_collection tlog_dir use_compression =
  let x = new mem_tlog_collection tlog_dir use_compression in
  let x2 = (x :> tlog_collection) in
  Lwt.return x2
