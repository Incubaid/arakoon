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


class mem_tlog_collection tlog_dir use_compression name =
object (self: #tlog_collection)

  val mutable data = []
  val mutable last_entry = (None: Entry.t option)

  method validate_last_tlog () =
    Lwt.return (TlogValidComplete, last_entry, None)

  method get_infimum_i () = Lwt.return Sn.start

  method get_last_i () =
    match last_entry with
      | None -> Sn.start
      | Some entry -> Entry.i_of entry 

  method get_last_value i =
    match last_entry with
      | None -> None
      | Some entry ->
        let i' = Entry.i_of entry in
	    begin
	      if i = i'
	      then 
            let v = Entry.v_of entry in
            Some v
	      else
	        None
	    end
          
  method get_last () =
    match last_entry with
      | None -> None
      | Some e -> Some (Entry.v_of e, Entry.i_of e)


  method iterate i last_i f =
    let data' = List.filter 
      (fun entry -> 
        let ei = Entry.i_of entry 
        (*and eu = Entry.u_of entry*)
        in
        ei >= i && ei <= last_i) data in
    Lwt_list.iter_s f (List.rev data')

      
  method get_tlog_count() = failwith "not supported"

  method dump_tlog_file start_i oc = failwith "not supported"

  method save_tlog_file name length ic = failwith "not supported"


  method log_value_explicit i (v:Value.t) sync marker =
    let entry = Entry.make i v 0L marker in
    let () = data <- entry::data in
    let () = last_entry <- (Some entry) in
    Lwt.return ()

  method log_value i v = self #log_value_explicit i v false None

          
  method dump_head oc = Llio.lwt_failfmt "not implemented"
  method save_head ic = Llio.lwt_failfmt "not implemented"

  method get_head_name () = failwith "not implemented"

  method get_tlog_from_name n = failwith "not implemented"
  
  method get_tlog_from_i = failwith "not implemented"   
  
  method close () = Lwt.return ()
  
  method remove_oldest_tlogs count = Lwt.return ()

  method remove_below i = Lwt.return ()
end

let make_mem_tlog_collection tlog_dir use_compression name =
  let x = new mem_tlog_collection tlog_dir use_compression name in
  let x2 = (x :> tlog_collection) in
  Lwt.return x2
