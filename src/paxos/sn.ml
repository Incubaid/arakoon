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

type t = Int64.t
let start = 0L
let succ t = Int64.succ t
let pred t = Int64.pred t
let compare = Int64.compare
let mul = Int64.mul
let div = Int64.div 
let add = Int64.add
let sub = Int64.sub 
let rem = Int64.rem

let diff a b = Int64.abs (Int64.sub a b) 
let of_int = Int64.of_int 
let to_int = Int64.to_int

let string_of = Int64.to_string
let of_string = Int64.of_string
let sn_to buf sn=  Llio.int64_to buf sn
let sn_from string p = Llio.int64_from string p
  
let output_sn oc t = Llio.output_int64 oc t
let input_sn ic = Llio.input_int64 ic


