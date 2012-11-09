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


(*

let response_ok_int64 oc i64 =
  Llio.output_int32 oc 0l >>= fun () ->
  Llio.output_int64 oc i64 >>= fun () ->
  Lwt.return false


*)

(*
let response_rc_bool oc rc b =
  Llio.output_int32 oc rc >>= fun () ->
  Llio.output_bool oc b >>= fun () ->
  Lwt.return false
*)

(*

*)
