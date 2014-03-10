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

type rc =
  | E_OK
  | E_NO_MAGIC
  | E_TOO_MANY_DEAD_NODES
  | E_NO_HELLO
  | E_NOT_MASTER
  | E_NOT_FOUND
  | E_WRONG_CLUSTER
  | E_ASSERTION_FAILED
  | E_READ_ONLY
  | E_OUTSIDE_INTERVAL
  | E_GOING_DOWN
  | E_NOT_SUPPORTED
  | E_NO_LONGER_MASTER
  | E_INCONSISTENT_READ
  | E_MAX_CONNECTIONS
  | E_UNKNOWN_FAILURE

val int32_of_rc : rc -> int32

val rc_of_int32: int32 -> rc

exception Exception of rc * string

val output_exception : Lwt_io.output_channel -> rc -> string -> unit Lwt.t
