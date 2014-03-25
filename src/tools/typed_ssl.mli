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

type 'a t

val create_client_context : Ssl.protocol -> [ `Client ] t
val create_server_context : Ssl.protocol -> [ `Server ] t
val create_both_context : Ssl.protocol -> [ `Client | `Server ] t

val embed_socket : Unix.file_descr -> 'a t -> Ssl.socket

val use_certificate : 'a t -> string -> string -> unit
val set_verify : 'a t -> Ssl.verify_mode list -> Ssl.verify_callback option -> unit
val set_client_CA_list_from_file : [> `Server ] t -> string -> unit
val load_verify_locations : 'a t -> string -> string -> unit

module Lwt : sig
  val ssl_connect : Lwt_unix.file_descr -> [> `Client ] t -> (Ssl.socket * Lwt_ssl.socket) Lwt.t
  val ssl_accept : Lwt_unix.file_descr -> [> `Server ] t -> (Ssl.socket * Lwt_ssl.socket) Lwt.t
end
