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

open Lwt
open Registry

let s2i = int_of_string
let i2s = string_of_int

let update_max db po =
  let _k = "max" in
  let v =
    try let s = db#get _k in s2i s
    with Not_found -> 0
  in
  let v' = match po with
    | None -> 0
    | Some s ->
      try s2i s
      with _ -> raise (Arakoon_exc.Exception(Arakoon_exc.E_UNKNOWN_FAILURE, "invalid arg"))
  in
  let m = max v v' in
  let ms = i2s m in
  db#set _k ms;
  Some (i2s m)




let () = Registry.register "update_max" update_max
