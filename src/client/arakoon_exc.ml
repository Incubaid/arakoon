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
  | E_UNKNOWN_FAILURE

let int32_of_rc = function
  | E_OK                  -> 0x00l
  | E_NO_MAGIC            -> 0x01l
  | E_TOO_MANY_DEAD_NODES -> 0x02l
  | E_NO_HELLO            -> 0x03l
  | E_NOT_MASTER          -> 0x04l
  | E_NOT_FOUND           -> 0x05l
  | E_WRONG_CLUSTER       -> 0x06l
  | E_ASSERTION_FAILED    -> 0x07l
  | E_READ_ONLY           -> 0x08l
  | E_UNKNOWN_FAILURE     -> 0xffl

let rc_of_int32 = function
  | 0x00l -> E_OK
  | 0x01l -> E_NO_MAGIC
  | 0x02l -> E_TOO_MANY_DEAD_NODES
  | 0x03l -> E_NO_HELLO
  | 0x04l -> E_NOT_MASTER
  | 0x05l -> E_NOT_FOUND
  | 0x06l -> E_WRONG_CLUSTER
  | 0x07l -> E_ASSERTION_FAILED
  | 0x08l -> E_READ_ONLY
  | 0xffl -> E_UNKNOWN_FAILURE
  | _     -> E_UNKNOWN_FAILURE

exception Exception of rc * string

open Lwt

let output_exception oc rc msg = 
  Llio.output_int32 oc (int32_of_rc rc) >>= fun () ->
  Llio.output_string oc msg
