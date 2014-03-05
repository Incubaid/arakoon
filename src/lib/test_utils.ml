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

module type QuickCheckable = sig
  type t
  val arbitrary : t QuickCheck.arbitrary
end

module QuickCheckUtils : sig
  module type Testable = sig
    type t
    val show : t -> string
    include QuickCheckable with type t := t
  end

  type 'a test

  val prop_test :
    (module Testable with type t = 'a) -> ('a -> bool) -> 'a test

  val quickCheck : 'a test -> QuickCheck.testresult
  val verboseCheck : 'a test -> QuickCheck.testresult

  val (>::~) : string -> 'a test -> OUnit.test
end = struct
  type 'a test = ('a -> bool) QuickCheck.testable * ('a -> bool)

  module type Testable = sig
    type t
    val show : t -> string
    include QuickCheckable with type t := t
  end

  let prop_test (type t) (module M : Testable with type t = t) prop =
    let testable = QuickCheck.testable_fun
      M.arbitrary
      M.show
      QuickCheck.testable_bool
    in
    testable, prop

  let quickCheck (t, p) = QuickCheck.quickCheck t p
  let verboseCheck (t, p) = QuickCheck.verboseCheck t p

  let (>::~) name t =
    let run () =
      match quickCheck t with
        | QuickCheck.Success -> ()
        | QuickCheck.Failure _ -> OUnit.assert_failure "QuickCheck: Failure"
        | QuickCheck.Exhausted _ -> OUnit.assert_failure "QuickCheck: Exhausted"
    in
    OUnit.(name >:: run)
end
