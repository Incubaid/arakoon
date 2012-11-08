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
open Interval
open Routing
open Statistics
open Ncfg
open Client_cfg

let read_command (ic,oc) =
  Llio.input_int32 ic >>= fun masked ->
  let magic = Int32.logand masked _MAGIC in
  begin
    if magic <> _MAGIC
    then
      begin
	Llio.output_int32 oc 1l >>= fun () ->
	Llio.lwt_failfmt "%lx has no magic" masked
      end
    else
      begin
	let as_int32 = Int32.logand masked _MASK in
	try
	  let c = lookup_code as_int32 in
          Lwt.return c
	with Not_found ->
          Llio.output_int32 oc 5l >>= fun () ->
	  let msg = Printf.sprintf "%lx: command not found" as_int32 in
	  Llio.output_string oc msg >>= fun () ->
          Lwt.fail (Failure msg)
      end
  end


let response_ok_unit oc =
  Llio.output_int32 oc 0l >>= fun () ->
  Lwt.return false

let response_ok_int64 oc i64 =
  Llio.output_int32 oc 0l >>= fun () ->
  Llio.output_int64 oc i64 >>= fun () ->
  Lwt.return false

let response_rc_string oc rc string =
  Llio.output_int32 oc rc >>= fun () ->
  Llio.output_string oc string >>= fun () ->
  Lwt.return false

let response_ok_string oc string = 
  Llio.output_int32 oc 0l >>= fun () ->
  Llio.output_string oc string >>= fun () ->
  Lwt.return false

let response_ok_string_option oc so =
  Llio.output_int32 oc 0l >>= fun () ->
  Llio.output_string_option oc so >>= fun () ->
  Lwt.return false


let response_rc_bool oc rc b =
  Llio.output_int32 oc rc >>= fun () ->
  Llio.output_bool oc b >>= fun () ->
  Lwt.return false

let handle_exception oc exn=
  let rc, msg, is_fatal, close_socket = match exn with
  | XException(Arakoon_exc.E_NOT_FOUND, msg) -> Arakoon_exc.E_NOT_FOUND,msg, false, false
  | XException(Arakoon_exc.E_ASSERTION_FAILED, msg) ->
    Arakoon_exc.E_ASSERTION_FAILED, msg, false, false
  | XException(rc, msg) -> rc,msg, false, true
  | Not_found -> Arakoon_exc.E_NOT_FOUND, "Not_found", false, false
  | Server.FOOBAR -> Arakoon_exc.E_UNKNOWN_FAILURE, "unkown failure", true, true
  | _ -> Arakoon_exc.E_UNKNOWN_FAILURE, "unknown failure", false, true
  in
  Lwt_log.error_f "Exception during client request (%s)" (Printexc.to_string exn) >>= fun () ->
  Arakoon_exc.output_exception oc rc msg >>= fun () ->
  begin
	  if close_socket
	  then Lwt_log.debug "Closing client socket" >>= fun () -> Lwt_io.close oc
	  else Lwt.return ()
  end >>= fun () ->
  if is_fatal
  then Lwt.fail exn
  else Lwt.return close_socket



