(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
      | Branch(l, _s, r) ->
        begin
          let b = _walk l in
          if b
          then b
          else _walk r
        end
      | Cluster x when x = cid -> true
      | Cluster _x -> false
    in _walk t

  let rec get_cluster_ids = function
    | Cluster x -> [x]
    | Branch (l, _s, r) ->
      (get_cluster_ids l) @ (get_cluster_ids r)

  let rec get_lower_sep parent_sep c_id = function
    | Branch(_l, s, r) when r = (Cluster c_id) ->
      Some s
    | Branch(l, _s, _r) when l = (Cluster c_id) ->
      parent_sep
    | Branch (l, s, r) ->
      let m_l = get_lower_sep None c_id l in
      begin
        match m_l with
          | None -> get_lower_sep (Some s) c_id r
          | _ -> m_l
      end
    | Cluster _ -> None

  let rec get_upper_sep parent_sep c_id = function
    | Branch(l, s, _) when l = (Cluster c_id) ->
      Some s
    | Branch(_, _, r) when r = (Cluster c_id) ->
      parent_sep
    | Branch(l, s, r) ->
      let m_l = get_upper_sep (Some s) c_id l in
      begin
        match m_l with
          | None -> get_upper_sep None c_id r
          | _ -> m_l
      end
    | Cluster _ -> None

  let remove t c_id =
    let rec inner_remove t c_id =
      match t with
        | Branch (l, s, r) ->
          let new_l = inner_remove l c_id in
          let new_r = inner_remove r c_id in
          begin
            match new_l, new_r with
              | None, None -> failwith "cluster_id present in two leaves??"
              | Some nl, Some nr -> Some (Branch( nl, s, nr))
              | None, Some nr -> Some nr
              | Some nl, None -> Some nl
          end
        | Cluster c  when c = c_id -> None
        | Cluster c  when c <> c_id -> Some (Cluster c)
        | Cluster _ -> failwith "I hate ocaml's comparison"
    in
    let m_result  = inner_remove t c_id in
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
    | Branch (l, _, _) ->
      get_left_most l
    | Cluster x -> x

  let rec get_right_most = function
    | Branch (_, _, r) ->
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
        | Branch(l,_, r) when (get_right_most l) = left && (get_left_most r) = right ->
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

  let routing_from buf =
    let rec _build () =
      let typ = Llio.bool_from buf in
      let r = if typ
        then let s   = Llio.string_from buf in Cluster s
        else let sep = Llio.string_from buf in
          let left  = _build () in
          let right = _build () in
          Branch(left,sep,right)
      in r
    in
    _build ()

  let serialized_size routing =
    let rec walk = function
      | Cluster x->
         1 + Llio.string_ssize x
      | Branch (left, sep, right) ->
         1 + Llio.string_ssize sep
         + walk left
         + walk right
    in
    walk routing

  let output_routing oc routing =
    let buf = Buffer.create 97 in
    let () = routing_to buf routing in
    let s = Buffer.contents buf in
    Llio.output_string oc s

  open Lwt

  let input_routing ic =
    Llio.input_string ic >>= fun s ->
    let buf = Llio.make_buffer s 0 in
    let r   = routing_from buf in
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
      | Cluster _ -> None
      | Branch (l, _, r) when l = (Cluster cluster_id) ->
        Some(get_left_most r)
      | Branch (_, _, r) when r = (Cluster cluster_id) ->
        begin
          match acc with
            | None -> failwith (Printf.sprintf "Cluster %s has no next" cluster_id)
            | Some t -> Some (get_left_most t)
        end
      | Branch (l, _, r) ->
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
      | Cluster _ -> None
      | Branch(l, _, r) when r = (Cluster cluster_id) ->
        Some (get_right_most l)
      | Branch(l, _, _) when l = (Cluster cluster_id) ->
        begin
          match acc with
            | None -> failwith (Printf.sprintf "Cluster %s has no previous" cluster_id)
            | Some t -> Some (get_right_most t)
        end
      | Branch(l, _, r) ->
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
      if not ( (contains t left) || (contains t right) )
      then
        failwith "Can only add one cluster at the time"
      else
        begin
          let up_sep = get_upper_sep None left t in
          let low_sep = get_lower_sep None right t in
          Lwt.ignore_result (
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
