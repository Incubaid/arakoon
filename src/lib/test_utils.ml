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
