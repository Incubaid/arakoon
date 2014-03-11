(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2013 Incubaid BVBA

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

open Test_utils

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

  module Test : sig
    module type S = sig
      type t
      include QuickCheckUtils.Testable with type t := t
      include Equable with type t := t
    end

    val quickCheck :
      (module S with type t = 'a) -> 'a QuickCheckUtils.test
  end
end = struct
  module type S = sig
    type s
    val (=:) : s -> s -> bool
  end

  module Make = functor(S : S) -> struct
    type t = S.s

    let (=:) = S.(=:)
    let (<>:) a b = not S.(a =: b)
  end

  module Default = functor(S : sig type s end) -> struct
    type t = S.s

    let (=:) = (=)
    let (<>:) = (<>)
  end

  module Test = struct
    module type S = sig
      type t
      include QuickCheckUtils.Testable with type t := t
      include Equable with type t := t
    end

    let quickCheck (type t)
      (module M : S with type t = t) =
      let open M in

      QuickCheckUtils.prop_test (module M) (fun v -> v =: v)
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
    type t = S.t

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
let compose f g = fun x -> f (g x)

module String = struct
  include String
  module C = CompareLib.Default(String)
  include C
end

module List = struct
  include List

  let is_empty = function
    | [] -> true
    | _ -> false

  let find' test list = try Some(find test list) with Not_found -> None
end

module Map = Extended_map

type direction =
    Map.direction =
  | Left
  | Right

type 'a counted_list = (int * 'a list)
