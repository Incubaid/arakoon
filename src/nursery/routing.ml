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

module Routing = struct
  type sep = string
  type t = 
    | Cluster of string
    | Branch of (t * sep * t)

  type repr = ((string * sep) list) * string
	
  let rec to_s = function
    | Cluster a -> Printf.sprintf "{%s}" a
    | Branch (left,b,right) -> 
      Printf.sprintf "%s,%s,%s" 
	(to_s left) b (to_s right)

 
  let rec build (sss, last) = 
    let  split sss = 
      let rec loop head rest = function
      | 0 -> List.rev head, rest
      | i -> loop (List.hd rest :: head) (List.tl rest) (i-1)
    in let len = List.length sss in
       loop [] sss (len/2)
  in
  match sss with
    | [] -> Cluster last
    | sss -> let left,right = split sss in
	     let cn,sep = List.hd right in
	     let right' = List.tl right in
	     let lr  = left, cn in
	     let rr  = right', last in
	     let lt = build lr in
	     let rt = build rr in
	     Branch (lt, sep, rt)

  let routing_to buf cfg = 
    let rec walk = function
      | Cluster x -> 
	Llio.bool_to buf true; 
	Llio.string_to buf x
      | Branch(left,sep,right) ->
	Llio.bool_to buf false; 
	Llio.string_to buf sep;
	walk left;
	walk right
    in
    walk cfg
      
  let routing_from buf pos = 
    let rec _build pos =     
      let typ,p1 = Llio.bool_from buf pos in
      let r,p = if typ 
	then let s,p2 = Llio.string_from buf p1 in Cluster s,p2
	else let sep,p2 = Llio.string_from buf p1 in
	     let left,p3 = _build p2 in
	     let right,p4 = _build p3 in
	     Branch(left,sep,right),p4
      in r,p
    in
    _build pos

  let output_routing oc routing = 
    let buf = Buffer.create 97 in
    let () = routing_to buf routing in
    let s = Buffer.contents buf in
    Llio.output_string oc s

  open Lwt

  let input_routing ic = 
    Llio.input_string ic >>= fun s ->
    let r,_ = routing_from s 0 in
    Lwt.return r
    
  let find cfg key = 
    let rec go = function
      | Cluster x -> x
      | Branch (left, sep, right) -> 
	if key < sep 
	then go left 
	else go right
    in go cfg
end
