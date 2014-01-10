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

let p_string s = Scanf.sscanf s "%s" (fun s -> s)

let p_int s = Scanf.sscanf s "%i" (fun i -> i)
let p_float s = Scanf.sscanf s "%f" (fun f -> f)

let p_string_list s = Str.split (Str.regexp "[, \t]+") s

let p_bool s = Scanf.sscanf s "%b" (fun b -> b)

let p_option p s = Some (p s)

let required section name = failwith  (Printf.sprintf "missing: %s %s" section name)

let default x = fun _ _ -> x

let get inifile section name s2a not_found =
  try
    let v_s = inifile # getval section name in
    s2a v_s
  with (Inifiles.Invalid_element _) -> not_found section name
