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

open OUnit
open Lwt
open Update
open Extra

let wrap_tlc = Tlogcollection_test.wrap Tlc2.make_tlc2

let suite = "tlc2" >:::[

  "empty_collection" >:: wrap_tlc Tlogcollection_test.test_empty_collection;
  "rollover" >:: wrap_tlc Tlogcollection_test.test_rollover;
  "get_value_bug" >:: wrap_tlc Tlogcollection_test.test_get_value_bug;
  "test_restart" >:: wrap_tlc Tlogcollection_test.test_restart;
  "test_iterate" >:: wrap_tlc Tlogcollection_test.test_iterate;
  "test_iterate2" >:: wrap_tlc Tlogcollection_test.test_iterate2;
  "validate" >:: wrap_tlc Tlogcollection_test.test_validate_normal;
  "validate_corrupt" >:: wrap_tlc Tlogcollection_test.test_validate_corrupt_1;

]
