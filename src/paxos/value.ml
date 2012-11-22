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

type t = 
  | Vc of Update.t
  | Vm of (string * int64)

let create_client_value (u:Update.t) = Vc u
let create_master_value (m,l) = Vm (m,l)

let is_master_set  = function
  | Vm _ ->  true
  | _ -> false

let is_synced = function 
  | Vm _ -> false
  | Vc u -> Update.is_synced u

let clear_master_set =function
    | Vm (m,l) -> Vm(m,0L) 
    | v -> v

let update_from_value = function
  | Vc u -> u
  | Vm (m,l) -> Update.MasterSet(m,l)

let value_to buf v= 
  let () = Llio.int_to buf 0xff in
  match v with
    | Vc u     -> Llio.char_to buf 'c'; Update.to_buffer buf u
    | Vm (m,l) -> 
        begin
          Llio.char_to buf 'm'; 
          Llio.string_to buf m;
          Llio.int64_to buf l
        end

let value_from string pos = 
  let i0,p1 = Llio.int_from string pos in
  if i0 = 0xff 
  then 
    let c,p2 = Llio.char_from string p1 in
    match c with
      | 'c' -> let u, p3 = Update.from_buffer string  p2 in
               (Vc u), p3
      | 'm' -> let m, p3 = Llio.string_from string p2 in
               let l, p4 = Llio.int64_from string p3 in
               (Vm (m,l)), p4
      | _ -> failwith "demarshalling error"
  else
    begin
      (* this is for backward compatibility: 
         formerly, we logged updates iso values *)
      let u,p2 = Update.from_buffer string pos in
      (Vc u), p2
    end;;
      
      

let value2s ?(values=false) = function
  | Vc u     -> Printf.sprintf "(Vc %s)" (Update.update2s u ~values)
  | Vm (m,l) -> Printf.sprintf "(Vm (%s,%Li)" m l
