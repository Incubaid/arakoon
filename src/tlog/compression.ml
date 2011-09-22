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

let archive_name tlog_name = tlog_name ^ ".aar" 
let tlog_name archive_name = 
  let len = String.length archive_name in
  let ext = String.sub archive_name (len-4) 4 in
  assert (ext=".aar");
  String.sub archive_name 0 (len-4)
    
let compress_tlog tlog_name archive_name =
  let limit = 896 * 1024 in
  let buffer_size = limit + (64 * 1024) in
  Lwt_io.with_file ~mode:Lwt_io.input tlog_name 
    (fun ic ->
      Lwt_io.with_file ~mode:Lwt_io.output archive_name
	(fun oc ->  
	  let rec fill_buffer (buffer:Buffer.t) (last_i:Sn.t) (counter:int) = 
	    Lwt.catch
	      (fun () -> 
		Tlogcommon.read_entry ic >>= fun (i,u) ->
		Tlogcommon.entry_to buffer i u;
		let (last_i':Sn.t) = if i > last_i then i else last_i 
		in
		if Buffer.length buffer < limit || counter = 0
		then fill_buffer (buffer:Buffer.t) last_i' (counter+1)
		else Lwt.return (last_i',counter)
	      )
	      (function 
		| End_of_file -> Lwt.return (last_i,counter)
		| exn -> Lwt.fail exn
	      )
	  in
	  let compress_and_write last_i buffer = 
	    let contents = Buffer.contents buffer in
	    let t0 = Unix.gettimeofday () in
	    Lwt_preemptive.detach 
	      (fun () ->
		let output = Bz2.compress ~block:9 contents 0 (String.length contents) 
		in
		output) () 
	    >>= fun output ->
	    Lwt_log.debug_f "compressed %i bytes into %i" 
	      (String.length contents) (String.length output) 
	    >>= fun () ->
	    Llio.output_int64 oc last_i >>= fun () ->
	    Llio.output_string oc output >>= fun () ->
	    let t1 = Unix.gettimeofday() in
	    let d = t1 -. t0 in
	    Lwt_unix.sleep (2.0 *. d) (* consume ~ 1/3  of the time *)
	  in
	  let buffer = Buffer.create buffer_size in
	  let rec loop () = 
	    fill_buffer buffer (-1L) 0 >>= fun (last_i,counter) ->
	    if counter = 0 
	    then Lwt.return ()
	    else
	      begin
		compress_and_write last_i buffer >>= fun () ->
		let () = Buffer.clear buffer in
		loop ()
	      end
	  in
	  loop ()
	)
    )
    
let uncompress_tlog archive_name tlog_name = 
  Lwt_io.with_file ~mode:Lwt_io.input archive_name
    (fun ic ->
      Lwt_io.with_file ~mode:Lwt_io.output tlog_name
	(fun oc ->
	  let rec loop () = 
	      Lwt.catch
		(fun () -> 
		  Sn.input_sn ic >>= fun last_i ->
		  Llio.input_string ic >>= fun compressed -> 
		  Lwt.return (Some compressed))
		(function 
		  | End_of_file -> Lwt.return None
		  | exn -> Lwt.fail exn
		)
	      >>= function 
		| None -> Lwt.return () 
		| Some compressed ->
		  begin
		    let lc = String.length compressed in
		    let output = Bz2.uncompress compressed 0 lc in
		    let lo = String.length output in
		    Lwt_io.write_from_exactly oc output 0 lo >>= fun () ->
		    loop ()
		  end
	  in loop ()))
    
