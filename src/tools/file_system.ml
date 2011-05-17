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

let copy_file source target = (* LOOKS LIKE Clone.copy_stream ... *)
  Lwt_log.debug_f "copy_file %s %s" source target >>= fun () ->
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
    loop () >>= fun () ->
    Lwt_log.debug "done: copy_file" 
  in
  Lwt_io.with_file ~mode:Lwt_io.input source
    (fun ic ->
      Lwt_io.with_file ~mode:Lwt_io.output target 
	(fun oc ->copy_all ic oc)
    )

let rename source target = 
  Lwt_log.debug_f "rename %s -> %s" source target >>= fun () ->
  Lwt_preemptive.detach (fun () -> Unix.rename source target) () >>= fun () ->
  Lwt.return ()

let stat filename = 
  Lwt_log.debug_f "stat %s" filename >>= fun () ->
  Lwt_preemptive.detach Unix.stat filename 

let exists filename = 
  Lwt.catch 
    (fun () -> stat filename >>= fun _ -> Lwt.return true)
    (function 
      | Unix.Unix_error (Unix.ENOENT,_,_) -> Lwt.return false
      | e -> Lwt.fail e
    )
