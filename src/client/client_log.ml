(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

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

let debug_void _x = Lwt.return ()

let debug_f_void fmt =
  Printf.CamlinternalPr.Tformat.kapr (fun _ -> Obj.magic (fun _ -> Lwt.return ())) fmt

let lwt_log_enabled = ref false

let enable_lwt_logging_for_client_lib_code () =
  lwt_log_enabled := true

let debug x =
  if !lwt_log_enabled then Lwt_log.debug x else debug_void x

let debug_f x =
  if !lwt_log_enabled then Lwt_log.debug_f x else debug_f_void x
