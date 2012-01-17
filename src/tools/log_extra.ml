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

let option_to_string f = function
  | None -> "None"
  | Some v -> "Some (" ^ String.escaped (f v ) ^ ")"


let string_option_to_string = option_to_string (fun s -> s)
let int_option_to_string = option_to_string string_of_int
let p_option = string_option_to_string

let string_of_list e_to_s list =
  let inner =
    List.fold_left (fun acc a -> acc ^ (e_to_s a) ^ ";") "" list
  in "[" ^ inner ^ "]"

let log_o o x =
  let k s = let os = o # to_string () in
    Client_log.debug  (os ^": " ^  s)
  in
  Printf.ksprintf k x
