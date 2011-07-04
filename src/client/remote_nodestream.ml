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

open Update
open Common
open Lwt


class type nodestream = object
  method iterate: 
    Sn.t -> (Sn.t * Update.t -> unit Lwt.t) ->
    Tlogcollection.tlog_collection ->
    head_saved_cb:(string -> unit Lwt.t) -> unit Lwt.t

  method collapse: int -> unit Lwt.t

end


class remote_nodestream (ic,oc) = 
  let request f =
    let buf = Buffer.create 32 in
    let () = f buf in
    Lwt_io.write oc (Buffer.contents buf) >>= fun () ->
    Lwt_io.flush oc
  in
object(self :# nodestream)

  method iterate (i:Sn.t) (f: Sn.t * Update.t -> unit Lwt.t)  
    (tlog_coll: Tlogcollection.tlog_collection) 
    ~head_saved_cb
    =
    let outgoing buf =
      command_to buf LAST_ENTRIES;
      Sn.sn_to buf i
    in
    let incoming ic =
      let save_head () = tlog_coll # save_head ic in
      let rec loop_entries () =
	Lwt_log.debug "loop_entries" >>= fun () ->
	Sn.input_sn ic >>= fun i2 ->
	Lwt_log.debug_f "i2=%s" (Sn.string_of i2) >>= fun () ->
	begin
	  if i2 = (-1L) then
	    Lwt.return ()
	  else
	    begin
	      Llio.input_int32 ic >>= fun chksum ->
	      Llio.input_string ic >>= fun entry ->	      
	      let update,_ = Update.from_buffer entry 0 in
	      let i = i2 in
	      f (i,update) >>= fun () ->
              loop_entries ()
	    end
	end
      in 
      Llio.input_int ic >>= function
	| 1 -> loop_entries ()
	| 2 -> 
	  begin 
	    save_head () >>= fun () -> 
	    let hf_name = tlog_coll # get_head_filename () in
	    head_saved_cb hf_name >>= fun () ->
	    loop_entries ()
	  end
	| x -> Llio.lwt_failfmt "don't know what %i means" x
    in
    request outgoing >>= fun () ->
    response ic incoming  


  method collapse n =
    let outgoing buf =
      command_to buf COLLAPSE_TLOGS;
      Llio.int_to buf n
    in
    let incoming ic =
      Llio.input_int ic >>= fun collapse_count ->
      let rec loop i =
      	if i = 0 
        	then Lwt.return ()
	      else 
      	  begin
                  Llio.input_int ic >>= function
                  | 0 ->
	            Llio.input_int64 ic >>= fun took ->
	            Lwt_log.debug_f "collapsing one file took %Li" took >>= fun () ->
	            loop (i-1)
                  | e ->
                    Llio.input_string ic >>= fun msg ->
                    Llio.lwt_failfmt "%s (EC: %d)" msg e
	        end
      in
      loop collapse_count
    in
    request outgoing >>= fun () ->
    response ic incoming


end

let prologue cluster connection =
  let (_,oc) = connection in 
  Llio.output_int32  oc _MAGIC >>= fun () ->
  Llio.output_int    oc _VERSION >>= fun () ->
  Llio.output_string oc cluster 

let make_remote_nodestream cluster connection = 
  prologue cluster connection >>= fun () ->
  let rns = new remote_nodestream connection in
  let a = (rns :> nodestream) in
  Lwt.return a
  
 
