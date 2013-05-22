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

open Store
open Lwt

module Batched_store = functor(S : Simple_store) ->
struct
  type t = { s : S.t }

  let with_transaction s f =
    S.with_transaction s.s f

  let exists s k =
    S.exists s.s k

  let set s t k v =
    S.set s.s t k v

  let get s k =
    S.get s.s k

  let delete s t k =
    S.delete s.s t k

  let range s prefix first finc last linc max =
    S.range s.s prefix first finc last linc max

  let range_entries s prefix first finc last linc max =
    S.range_entries s.s prefix first finc last linc max

  let rev_range_entries s prefix first finc last linc max =
    S.range_entries s.s prefix first finc last linc max

  let prefix_keys s prefix max =
    S.prefix_keys s.s prefix max

  let delete_prefix s t prefix =
    S.delete_prefix s.s t prefix

  let close s =
    S.close s.s

  let reopen s =
    S.reopen s.s

  let make_store b s =
    S.make_store b s >>= fun s -> Lwt.return { s }

  let get_location s =
    S.get_location s.s

  let relocate s =
    S.relocate s.s

  let get_key_count s =
    S.get_key_count s.s

  let optimize s =
    S.optimize s.s

  let defrag s =
    S.defrag s.s

  let copy_store s =
    S.copy_store s.s

  let copy_store2 =
    S.copy_store2

  let get_fringe s =
    S.get_fringe s.s
end

module Local_store = Batched_store(Local_store)

