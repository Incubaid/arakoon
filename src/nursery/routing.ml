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

  type range_direction =
    | UPPER_BOUND
    | LOWER_BOUND

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

  let change t from_cn sep to_cn =
    let rec _walk = function
      | Cluster n -> if n = from_cn || n = to_cn 
	then Branch(Cluster from_cn,sep, Cluster to_cn)
	else failwith "??"
      | Branch(Cluster l,s, Cluster r) when l = from_cn && r = to_cn ->
	Branch(Cluster l, sep, Cluster r)
      | Branch(l,s,r) ->
	if s < sep
	then Branch(l,s, _walk r)
	else Branch(_walk l,s,r)
    in
    _walk t
	  
	  
	  
	

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

  let next cfg key = 
    let rec go ok = function
      | Cluster x -> x
      | Branch (left, sep, right) ->
	if ok then
	  if key < sep 
	  then go ok left 
	  else go ok right
	else
	  if key > sep 
	  then go true right
	  else go ok left
    in go false cfg
  
  let contains t cid =
    let rec _walk = function 
      | Branch(l, s, r) ->
        begin
          let b = _walk l in
          if b 
          then b
          else _walk r
        end  
      | Cluster x when x = cid -> true
      | Cluster x -> false
    in _walk t
    
  let get_diff t left sep right =
    let cl = Cluster left in
    let cr = Cluster right in
    let rec _get_upper_sep parent_sep = function 
      | Branch(l, s, r) when l = cl ->
        Some s
      | Branch(l, s, r) when r = cl ->
        parent_sep
      | Branch(l, s, r) ->
        let m_l = _get_upper_sep None l in
        begin
          match m_l with
            | None -> _get_upper_sep None r 
            | _ -> m_l
        end
      | Cluster x -> None
    in 
    let rec _get_lower_sep parent_sep = function
      | Branch(l, s, r) when r = cr ->
        Some s
      | Branch(l, s, r) when l = cr ->
        parent_sep
      | Branch (l, s, r) ->
        let m_l = _get_lower_sep None l in
        begin
          match m_l with 
            | None -> _get_lower_sep None r
            | _ -> m_l 
        end
      | Cluster x -> None
    in
    let up_sep = _get_upper_sep None t in
    let low_sep = _get_lower_sep None t in 
    begin
      match (low_sep, up_sep) with
        | (Some x, Some y) when x = y ->
          if sep > x then
            right, left, UPPER_BOUND
          else 
            left, right, LOWER_BOUND
        | (Some x, None) when not (contains t right) -> 
          left, right, LOWER_BOUND
        | (None, Some y) when not (contains t left) ->
          right, left, UPPER_BOUND
        | _ ->
          failwith "Impossible routing change requested"
    end
end
