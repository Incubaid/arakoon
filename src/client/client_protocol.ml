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

open Common
open Lwt
open Lwt_log
open Log_extra
open Extra
open Update

let read_command (ic,oc) =
  debug "read_command" >>= fun () ->
  Llio.input_int32 ic >>= fun masked ->
    let magic = Int32.logand masked _MAGIC in
      begin
  if magic <> _MAGIC then
    Llio.output_int32 oc 1l >>= fun () ->
    Llio.lwt_failfmt "%lx has no magic" masked
  else
    let as_int32 = Int32.logand masked _MASK in
      try
        Lwt_log.debug_f "command=%08lx" as_int32 >>= fun () ->
        Lwt.return (List.assoc as_int32 int2code)
      with Not_found ->
        Llio.output_int32 oc 5l >>= fun () ->
    let msg = Printf.sprintf "%lx not found" as_int32 in
      Llio.output_string oc msg >>= fun () ->
        Lwt.fail (Failure msg)
      end


let response_ok_unit oc =
  Llio.output_int32 oc 0l


let response_rc_string oc rc string =
  Llio.output_int32 oc rc >>= fun () ->
  Llio.output_string oc string

let response_rc_bool oc rc b =
  Llio.output_int32 oc rc >>= fun () ->
  Llio.output_bool oc b

let handle_exception oc exn= 
  let rc, msg = match exn with
  | XException(rc, msg) -> rc,msg
  | Not_found -> Arakoon_exc.E_NOT_FOUND, "Not_found"
  | _ -> Arakoon_exc.E_UNKNOWN_FAILURE, "unknown failure"
  in 
  Arakoon_exc.output_exception oc rc msg

let one_command (ic,oc) backend =
  read_command (ic,oc) >>= function
    | HELLO ->
	begin
          Llio.input_string ic >>= fun client ->
	    Lwt_log.debug_f "HELLO %S" client >>= fun () ->
            backend # hello client >>= fun server ->
              response_rc_string oc 0l server
	end
    | EXISTS ->
      begin
	Llio.input_string ic >>= fun key ->
	Lwt_log.debug_f "EXISTS %S" key >>= fun () ->
	Lwt.catch
	  (fun () -> backend # exists key >>= fun exists ->
	    response_rc_bool oc 0l exists)
	  (handle_exception oc)
      end
    | GET ->
	begin
          Llio.input_string ic >>= fun  key ->
	  Lwt_log.debug_f "GET %S" key >>= fun () ->
	  Lwt.catch
	    (fun () -> backend # get key >>= fun value ->
	       response_rc_string oc 0l value)
	    (handle_exception oc)
	end
    | SET ->
	begin
          Llio.input_string ic >>= fun key ->
          Llio.input_string ic >>= fun value ->
	  Lwt_log.debug_f "SET %S:%S" key value >>= fun () ->
	  Lwt.catch
	    (fun () -> backend # set key value >>= fun () ->
	       response_ok_unit oc
	    )
	    (handle_exception oc)
	end
    | DELETE ->
	begin
          Llio.input_string ic >>= fun key ->
          Lwt_log.debug_f "DELETE %S" key >>= fun () ->
          Lwt.catch
	    (fun () ->
	       backend # delete key >>= fun () ->
	       response_ok_unit oc)
	    (handle_exception oc)
	end
    | RANGE ->
	begin
          Llio.input_string_option ic >>= fun (first:string option) ->
          Llio.input_bool          ic >>= fun finc  ->
          Llio.input_string_option ic >>= fun (last:string option)  ->
          Llio.input_bool          ic >>= fun linc  ->
          Llio.input_int           ic >>= fun max   ->
          Lwt_log.debug_f "RANGE %s %b %s %b %i" (p_option first) finc (p_option last) linc max
             >>= fun () ->
          Lwt.catch
	    (fun () ->
	       backend # range first finc last linc max >>= fun list ->
	       Llio.output_int32 oc 0l >>= fun () ->
               Llio.output_int oc (List.length list)  >>= fun () ->
               Lwt_list.iter_s (Llio.output_string oc) list >>= fun () ->
	       Lwt_io.flush oc
	    )
	    (handle_exception oc)
	end
    | RANGE_ENTRIES ->
	begin
	  Llio.input_string_option ic >>= fun first ->
	  Llio.input_bool          ic >>= fun finc  ->
	  Llio.input_string_option ic >>= fun last  ->
	  Llio.input_bool          ic >>= fun linc  ->
	  Llio.input_int           ic >>= fun max   ->
	  Lwt_log.debug_f "RANGE_ENTRIES %S %b %S %b %i" (p_option first) finc (p_option last) linc max
	    >>= fun () ->
          Lwt.catch
	    (fun () ->
	       backend # range_entries first finc last linc max
	       >>= fun (list:(string*string) list) ->
	       Llio.output_int32 oc 0l >>= fun () ->
		 let size = List.length list in
		 Lwt_log.debug_f "size = %i" size >>= fun () ->
	       Llio.output_int oc size >>= fun () ->
	       Lwt_list.iter_s
		 (fun (k,v) ->
		    Llio.output_string oc k >>= fun () ->
		    Llio.output_string oc v) list >>= fun () ->
	       Lwt_io.flush oc
	    )
	    (handle_exception oc)
	end
    | LAST_ENTRIES ->
	begin
	  Sn.input_sn ic >>= fun i ->
	  Llio.output_int32 oc 0l >>= fun () ->
	  let f (i,u) = Tlogcommon.write_entry oc i u in
	  backend # last_entries i f >>= fun () ->
	  Sn.output_sn oc (-1L) >>= fun () ->
          Lwt_io.flush oc
	end
    | WHO_MASTER ->
	begin
	Lwt_log.debug "WHO_MASTER" >>= fun () ->
	backend # who_master () >>= fun m ->
	  Llio.output_int32 oc 0l >>= fun () ->
	  Llio.output_string_option oc m >>= fun () ->
	  Lwt_io.flush oc
	end
    | TEST_AND_SET ->
	begin
	  Llio.input_string ic >>= fun key ->
	  Llio.input_string_option ic >>= fun expected ->
          Llio.input_string_option ic >>= fun wanted ->
	  backend # test_and_set key expected wanted >>= fun vo ->
	  Llio.output_int oc 0 >>= fun () ->
          Llio.output_string_option oc vo >>= fun () ->
	  Lwt_io.flush oc
	end
    | PREFIX_KEYS ->
      begin
	Llio.input_string ic >>= fun key ->
	Llio.input_int ic >>= fun max ->
	backend # prefix_keys key max >>= fun keys ->
        let size = List.length keys in
	Llio.output_int oc 0 >>= fun () ->
        Lwt_log.debug_f "size = %i" size >>= fun () ->
	Llio.output_int oc size >>= fun () ->
	Lwt_list.iter_s (Llio.output_string oc) keys >>= fun () ->
	Lwt_io.flush oc
      end
    | MULTI_GET ->
      begin
	Llio.input_int ic >>= fun length ->
	let rec loop keys i = 
	  if i = 0
	  then Lwt.return keys
	  else 
	    begin
	      Llio.input_string ic >>= fun key -> 
	      loop (key :: keys) (i-1)
	    end
	in 
	loop [] length >>= fun keys ->
	Lwt.catch
	  (fun () ->
	    backend # multi_get keys >>= fun values ->
	    Llio.output_int oc 0 >>= fun () ->
	    Llio.output_int oc length >>= fun () ->
	    Lwt_list.iter_s (Llio.output_string oc) values >>= fun () ->
	    Lwt_io.flush oc)
	  (handle_exception oc)
      end
    | SEQUENCE ->
      begin
	Llio.input_string ic >>= fun data ->
	let update,_ = Update.from_buffer data 0 in
	match update with
	  | Update.Sequence updates ->
	    Lwt.catch
	      (fun () ->
		begin
		  backend # sequence updates >>= fun () ->
		  response_ok_unit oc
		end)
	      (handle_exception oc)
	  | _ -> handle_exception oc 
	    (XException (Arakoon_exc.E_UNKNOWN_FAILURE,
			 "should have been a sequence"))
      end
      

let protocol backend connection =
  info "client_protocol" >>= fun () ->
  let _,oc = connection in
  let rec loop () =
    one_command connection backend >>= fun () ->
    Lwt_io.flush oc >>= fun() ->
    loop ()
  in
  loop () 
