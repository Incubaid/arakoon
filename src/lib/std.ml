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



module type Show = sig
  type t
  val show : t -> string
end

module type Equable = sig
  type t

  val (=:) : t -> t -> bool
  val (<>:) : t -> t -> bool
end

module EqLib : sig
  module type S = sig
    type s
    val (=:) : s -> s -> bool
  end

  module Make : functor(S : S) -> Equable with type t := S.s
  module Default : functor(S : sig type s end) -> Equable with type t := S.s

end = struct
  module type S = sig
    type s
    val (=:) : s -> s -> bool
  end

  module Make = functor(S : S) -> struct
    let (=:) = S.(=:)
    let (<>:) a b = not (a =: b)
  end

  module Default = functor(S : sig end) -> struct
    let (=:) = (=)
    let (<>:) = (<>)
  end

  
end

module Compare : sig
  type t = LT | EQ | GT
end = struct
  type t = LT | EQ | GT
end

module type Comparable = sig
  type t

  val (<:) : t -> t -> bool
  val (>:) : t -> t -> bool
  val (<=:) : t -> t -> bool
  val (>=:) : t -> t -> bool

  include Equable with type t := t

  val compare' : t -> t -> Compare.t

  val min : t -> t -> t
  val max : t -> t -> t
end

module CompareLib : sig
  module type S = sig
    type t
    val compare' : t -> t -> Compare.t
  end

  module Make : functor(S : S) -> Comparable with type t := S.t

  module type T = sig
    type t
    val compare : t -> t -> int
  end

  module Default : functor(T : T) -> Comparable with type t := T.t

  module type U = sig
    type s
  end

  module Default' : functor(U : U) -> Comparable with type t := U.s

  val wrap : ('a -> 'a -> int) -> 'a -> 'a -> Compare.t
end = struct
  open Compare

  module type S = sig
    type t
    val compare' : t -> t -> Compare.t
  end

  module Make = functor(S : S) -> struct
    let (<:) a b = match S.compare' a b with
      | LT -> true
      | EQ | GT -> false
    let (>:) a b = match S.compare' a b with
      | GT -> true
      | EQ | LT -> false
    let (<=:) a b = match S.compare' a b with
      | LT | EQ -> true
      | GT -> false
    let (>=:) a b = match S.compare' a b with
      | GT | EQ -> true
      | LT -> false
    let (=:) a b = match S.compare' a b with
      | EQ -> true
      | LT | GT -> false
    let (<>:) a b = match S.compare' a b with
      | EQ -> false
      | LT | GT -> true

    let compare' = S.compare'

    let max a b =
      if a >: b
      then
        a
      else
        b

    let min a b =
      if a >: b
      then
        b
      else
        a
  end

  let wrap cmp a b = match cmp a b with
    | -1 -> LT
    | 0 -> EQ
    | 1 -> GT
    | _ -> failwith "Impossible compare result"

  module type T = sig
    type t
    val compare : t -> t -> int
  end

  module Default = functor(T : T) ->
    Make(struct
      type t = T.t
      let compare' = wrap T.compare
    end)

  module type U = sig
    type s
  end

  module Default' = functor(U : U) ->
    Make(struct
      type t = U.s
      let compare' = wrap compare
    end)
end

external id : 'a -> 'a = "%identity"
let const v = fun _ -> v

module Option = struct
  type 'a t = 'a option =
            | None
            | Some of 'a

  let some x = Some x

  let get_some = function
    | None -> assert false
    | Some x -> x
end


module String = struct
  include String
  module C = CompareLib.Default(String)
  include C
end

module List = struct
  include List

  let split3 xs =
    let x0s_r, x1s_r, x2s_r =
      fold_left
        (fun (x0s,x1s,x2s) (x0,x1,x2) -> (x0::x0s, x1::x1s, x2::x2s))
        ([], [], []) xs
    in rev x0s_r, rev x1s_r, rev x2s_r
  let is_empty = function
    | [] -> true
    | _ -> false

  (** Lookup a key in a (key, value) list, and add, update or delete it

  For every (key, value) pair found in the list for which key equals the given
  key, the corresponding value is passed to f. When this call returns Some
  value, a pair of the key and this new value will occur in the result. If it
  returns None, the pair is removed from the result.

  If the key is not found in the list, the default value (if given) is added as
  (key, value) to the list.

  Ordering is not preserved.

  @param list Key-value list
  @param key Key to look-up
  @param default Optional default value if key isn't found
  @param f Update function *)
  let alter ?(equals=(=)) ~list ~key ?default f =
    let rec loop found acc = function
      | [] -> (found, acc)
      | ((k, v) as p :: r) ->
          if equals k key
            then match f v with
              | None -> loop true acc r
              | Some v' -> loop true ((k, v') :: acc) r
            else
              loop found (p :: acc) r
    in
    let (found, l) = loop false [] list in
    if found
      then l
      else match default with
        | None -> l
        | Some v -> (key, v) :: l


end

type direction =
  | Left
  | Right

module Map = struct
  module Make(Ord : Map.OrderedType) = struct
    module M = Map.Make(Ord)
    include M

    (* define same types as in original Map implementation,
       this allows us to access their contents with Obj.magic *)
    type key' = Ord.t
    type 'a t' =
      | Empty
      | Node of 'a t' * key' * 'a * 'a t' * int

    module Zipper = struct

      type 'a t =
          'a t' * ('a t' * direction) list

      let first ?(z=[]) t =
        let rec inner acc = function
          | Empty ->
             None
          | Node(Empty, _, _, _, _) as t ->
             Some (t, acc)
          | Node(l, _, _, _, _) as t ->
             inner ((t, Left)::acc) l in
        inner z (Obj.magic t)

      let last ?(z=[]) t =
        let rec inner acc = function
          | Empty -> None
          | Node(_, _, _, Empty, _) as t ->
             Some (t, acc)
          | Node(_, _, _, r, _) as t ->
             inner ((t, Right) :: acc) r in
        inner z (Obj.magic t)

      let get (h, _) =
        match h with
        | Empty ->
           failwith "invalid zipper"
        | Node(_, k, v, _, _) ->
           (k, v)

      let next (h, z) =
        match h with
        | Empty ->
           failwith "invalid zipper"
        | Node(_, _, _, Empty, _) ->
           begin
             let rec inner = function
               | [] -> None
               | (t, Left) :: z' -> Some (t, z')
               | (_, Right) :: z' -> inner z'
             in
             inner z
           end
        | Node(_, _, _, r, _) ->
           let z' = (h, Right) :: z in
           first ~z:z' r

      let prev (h, z) =
        match h with
        | Empty ->
           failwith "invalid zipper"
        | Node(Empty, _, _, _, _) ->
           begin
             let rec inner = function
               | (t, Right) :: z' ->
                  Some (t, z')
               | (_t, Left) :: z' ->
                  inner z'
               | [] ->
                  None in
             inner z
           end
        | Node(l, _, _, _, _) ->
           let z' = (h, Left) :: z in
           last ~z:z' l

      let jump ?(dir=Right) k t =
        let rec inner acc = function
          | Empty ->
             begin
               let rec inner = function
                 | (t, d) :: tl when d <> dir ->
                    Some (t, tl)
                 | _ :: tl ->
                    inner tl
                 | [] ->
                    None in
               inner acc
             end
          | Node(l, k', _v, r, _) as t ->
             begin
               match Ord.compare k k' with
               | -1 ->
                  inner ((t, Left) :: acc) l
               | 0 ->
                  Some (t, acc)
               | 1 ->
                  inner ((t, Right) :: acc) r
               | _ ->
                  failwith "impossible compare result"
             end in
        inner [] (Obj.magic t)
    end

    module Cursor = struct
      type 'a m = 'a t
      type 'a t = 'a Zipper.t option ref * 'a m

      let with_cursor m f =
        f (ref None, m)

      let is_valid (zo, _) =
        !zo <> None

      let last ((zo, m) as c) =
        zo := Zipper.last m;
        is_valid c

      let jump ?dir k ((zo, m) as c) =
        zo := Zipper.jump ?dir k m;
        is_valid c

      let next ((zo, _) as c) =
        match !zo with
        | None ->
           failwith "invalid cursor"
        | Some z ->
           zo := Zipper.next z;
           is_valid c

      let prev ((zo, _) as c) =
        match !zo with
        | None ->
           failwith "invalid cursor"
        | Some z ->
           zo := Zipper.prev z;
           is_valid c

      let get (zo, _) =
        match !zo with
        | None ->
           failwith "invalid cursor"
        | Some z ->
           Zipper.get z

    end
  end
end

type 'a counted_list = (int * 'a list)
