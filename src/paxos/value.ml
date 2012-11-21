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

open Update

type t = Vx of Update.t

let create_value (u:Update.t) = Vx u

let is_master_set (Vx u) = 
  match u with 
    | Update.MasterSet _ -> true
    | _ -> false

let update_from_value (Vx u) = u

let value_to buf (Vx u)= 
  let () = Llio.int_to buf 0xff in
  Update.to_buffer buf u
    
let value_from string pos = 
  let i0,p1 = Llio.int_from string pos in
  let () = assert (i0 = 0xff) in
  let u,p2 = Update.from_buffer string  p1 in
  (Vx u), p2

