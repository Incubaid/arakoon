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



type t = Int64.t
let start = 0L
let zero = 0L
let succ t = Int64.succ t
let pred t = Int64.pred t
let compare = Int64.compare

let mul = Int64.mul
let div = Int64.div

let add = Int64.add
let sub = Int64.sub
let rem = Int64.rem

let diff a b = Int64.abs (Int64.sub a b)
let of_int = Int64.of_int
let to_int = Int64.to_int

let string_of = Int64.to_string
let of_string = Int64.of_string
let sn_to buf sn=  Llio.int64_to buf sn
let sn_from buf = Llio.int64_from buf

let output_sn oc t = Llio.output_int64 oc t
let input_sn ic = Llio.input_int64 ic
