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

  let rec get_cluster_ids = function
    | Cluster x -> [x]
    | Branch (l, s, r) ->
      (get_cluster_ids l) @ (get_cluster_ids r)

  let rec get_lower_sep parent_sep c_id = function
      | Branch(l, s, r) when r = (Cluster c_id) ->
        Some s
      | Branch(l, s, r) when l = (Cluster c_id) ->
        parent_sep
      | Branch (l, s, r) ->
        let m_l = get_lower_sep None c_id l in
        begin
          match m_l with
            | None -> get_lower_sep (Some s) c_id r
            | _ -> m_l
        end
      | Cluster x -> None

  let rec get_upper_sep parent_sep c_id = function
      | Branch(l, s, r) when l = (Cluster c_id) ->
        Some s
      | Branch(l, s, r) when r = (Cluster c_id) ->
        parent_sep
      | Branch(l, s, r) ->
        let m_l = get_upper_sep (Some s) c_id l in
        begin
          match m_l with
            | None -> get_upper_sep None c_id r
            | _ -> m_l
        end
      | Cluster x -> None

(*
  let rec remove t c_id =
    match t with
      | Branch (l, s, r) when l = (Cluster c_id) ->
        r
      | Branch (l, s, r) when r = (Cluster c_id) ->
        l
      | Branch (l, s, r) ->
        let new_l = remove l c_id in
        let new_r = remove r c_id in
        Branch(new_l, s, new_r)
      | Cluster c ->
        Cluster c
*)

  let remove t c_id =
    let rec inner_remove t c_id other_side =
      match t with
        | Branch (l, s, r) ->
          let new_l = inner_remove l c_id (Some r) in
          let new_r = inner_remove r c_id (Some l) in
          begin
            match new_l, new_r with
              | None, None -> failwith "cluster_id present in two leaves??"
              | Some nl, Some nr -> Some (Branch( nl, s, nr))
              | None, Some nr -> Some nr
              | Some nl, None -> Some nl
          end
        | Cluster c  when c = c_id -> None
        | Cluster c  when c <> c_id -> Some (Cluster c)
        | Cluster c -> failwith "I hate ocaml's comparison"
    in
    let m_result  = inner_remove t c_id None in
    match m_result with
      | None -> failwith "Cannot remove last entry from routing"
      | Some r -> r

  let compact t =
    let clusters = get_cluster_ids t in
    let maybe_remove_cluster routing cluster_id=
      let upper = get_upper_sep None cluster_id routing in
      let lower = get_lower_sep None cluster_id routing in
      match upper,lower with
        | Some x, Some y when x = y -> remove routing cluster_id
        | _ -> routing
    in
    List.fold_left maybe_remove_cluster t clusters

  let rec get_left_most = function
    | Branch (l, s, r) ->
      get_left_most l
    | Cluster x -> x

  let rec get_right_most = function
    | Branch (l, s, r) ->
      get_right_most r
    | Cluster x -> x


  let change t left sep right =
    if not( contains t left || contains t right )
    then
      failwith "Cannot add two clusters at the same time"
    else
      let rec _walk = function
        | Cluster n ->
          begin
            if n = left || n = right
            then
              Branch(Cluster left,sep, Cluster right)
            else
              Branch(Cluster n, sep, Cluster left)
          end
        | Branch(l,s, r) when (get_right_most l) = left && (get_left_most r) = right ->
          Branch(l, sep, r)
        | Branch(l,s,r) ->
          if s < sep
          then Branch(l,s, _walk r)
          else Branch(_walk l,s,r)
      in
      let n_r = _walk t in
      compact n_r





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


  let next_cluster t cluster_id =
    let rec inner_next acc = function
      | Cluster x -> None
      | Branch (l, s, r) when l = (Cluster cluster_id) ->
        Some(get_left_most r)
      | Branch (l, s, r) when r = (Cluster cluster_id) ->
        begin
          match acc with
            | None -> failwith (Printf.sprintf "Cluster %s has no next" cluster_id)
            | Some t -> Some (get_left_most t)
        end
      | Branch (l, s, r) ->
        let m_l = inner_next (Some r) l in
        begin
          match m_l with
            | Some x -> Some x
            | None -> inner_next None r
        end
    in
    inner_next None t

  let prev_cluster t cluster_id =
    let rec inner_prev acc = function
      | Cluster x -> None
      | Branch(l, s, r) when r = (Cluster cluster_id) ->
        Some (get_right_most l)
      | Branch(l, s, r) when l = (Cluster cluster_id) ->
        begin
          match acc with
            | None -> failwith (Printf.sprintf "Cluster %s has no previous" cluster_id)
            | Some t -> Some (get_right_most t)
        end
      | Branch(l, s, r) ->
        let m_l = inner_prev None l in
        begin
          match m_l with
            | Some x -> Some x
            | None -> inner_prev (Some l) r
        end
    in
    inner_prev None t


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

  let get_diff t left sep right =
    begin
      if not ( (contains t left) or (contains t right) )
      then
        failwith "Can only add one cluster at the time"
      else
        begin
            let up_sep = get_upper_sep None left t in
            let low_sep = get_lower_sep None right t in
            Lwt_extra.ignore_result (
            Client_log.debug_f "lower,upper = %s,%s"
              (Log_extra.string_option2s low_sep)
              (Log_extra.string_option2s up_sep)
            );
            begin
              match (low_sep, up_sep) with
                | (Some x, Some y) when x = y ->
                  if sep > x then
                    right, left, UPPER_BOUND
                  else
                    left, right, LOWER_BOUND

            | (Some x, None) when not (contains t left) ->
              if sep > x
              then right, left, UPPER_BOUND
              else
                failwith "Invalid routing diff request"
                (*let cl = find t sep in
                cl, left, LOWER_BOUND *)

            | (None, Some y) when not (contains t right) ->
              if sep <= y
              then left, right, LOWER_BOUND
              else failwith "Invalid routing_diff requested"

            | (None, None) when not (contains t right) ->
              left, right, LOWER_BOUND
            | (None, None) when not (contains t left) ->
              right, left, UPPER_BOUND
                | _ ->
                  failwith "Impossible routing diff requested"
            end
      end

    end
end
