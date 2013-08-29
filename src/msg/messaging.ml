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
open Log_extra
open Message
open Lwt_buffer

type id = string
type address = (string * int)

class type messaging = object
  method register_receivers: (id * address) list -> unit
  method send_message: Message.t -> source:id -> target:id -> unit Lwt.t
  method recv_message: target:id -> (Message.t * id) Lwt.t
  method expect_reachable: target: id -> bool
  method run :
    ?setup_callback:(unit -> unit Lwt.t) ->
    ?teardown_callback:(unit -> unit Lwt.t) ->
    ?ssl_context:([> `Server ] Typed_ssl.t) ->
    unit -> unit Lwt.t
  method get_buffer: id -> (Message.t * id) Lwt_buffer.t
end
