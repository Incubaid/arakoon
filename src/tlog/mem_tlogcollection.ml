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

module Sn_map = Map.Make(
  struct
    type t = Sn.t 
    let compare = Sn.compare
  end);;

class mem_tlog_collection tlog_dir =
object (self: #tlog_collection)

  val mutable data = Sn_map.empty
  val mutable last_update = None

  method validate_last_tlog () =
    Lwt.return (TlogValidComplete, None)

  method validate () =
    Lwt.return (TlogValidComplete, None)

  method iterate i last_i f =
    let filtered = Sn_map.fold
      (fun k v a ->
	if k >= i then (k, v)::a else a
      ) data [] in
    Lwt_list.iter_s f filtered >>= fun () ->
    Lwt.return ()

  method  log_update i u =
    let () = data <- Sn_map.add i u data in
    let () = last_update <- (Some (i,u)) in
    Lwt.return Tlogwriter.WRSuccess

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

  method close () = Lwt.return ()
end

let make_mem_tlog_collection tlog_dir =
  let x = new mem_tlog_collection tlog_dir in
  let x2 = (x :> tlog_collection) in
  Lwt.return x2
