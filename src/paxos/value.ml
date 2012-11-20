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

type t = Vx of string

let create_value update =
    let b = Buffer.create 64 in
    let () = Update.to_buffer b update in
    let s = Buffer.contents b in
    Vx s

let is_master_set (Vx s) = 
  let kind,_ = Llio.int_from s 0 in
  kind = 4

let update_from_value (Vx s) =
    let u,_ = Update.from_buffer s 0 in
    u

let value_to buf (Vx s)= Llio.string_to buf s
    
let value_from string pos = 
  let s, pos1 = Llio.string_from string pos in 
     (Vx s), pos1

