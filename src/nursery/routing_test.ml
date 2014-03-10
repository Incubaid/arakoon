(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

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

open OUnit
open Routing

let test_serialization () =
  let repr =[("left"),"k";],"right" in
  let routing = Routing.build repr in
  Printf.eprintf "original=%s\n" (Routing.to_s routing);
  let b = Buffer.create 127 in
  Routing.routing_to b routing ;
  let flat = Buffer.contents b in
  let inflated,_ = Routing.routing_from flat 0 in
  Printf.eprintf "reinflated=%s\n" (Routing.to_s inflated);
  OUnit.assert_equal routing inflated ~printer:Routing.to_s;
  ()

let test_change1 () =
  let repr = ["left","k";], "right" in
  let r = Routing.build repr in
  let r2 = Routing.change r "left" "m" "right" in
  let () = Printf.eprintf "r2=%s\n" (Routing.to_s r2) in
  let n = Routing.find r2 "l" in
  OUnit.assert_equal ~printer:(fun s -> s) n "left"

let test_change2 () =
  let repr = ["one","d";
              "two","k";
              "three","t";
             ], "four" in
  let r  = Routing.build repr in
  let r2 = Routing.change r "three" "s" "four" in
  let () = Printf.eprintf "r =%s\n" (Routing.to_s r) in
  let () = Printf.eprintf "r2=%s\n" (Routing.to_s r2) in
  let n = Routing.find r2 "s1" in
  OUnit.assert_equal ~printer:(fun s -> s) n "four"

let suite = "routing" >::: [
    "serialization" >:: test_serialization;
    "change1" >:: test_change1;
    "change2" >:: test_change2;
  ]
