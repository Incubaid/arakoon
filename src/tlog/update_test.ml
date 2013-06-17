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
open Update
open Interval

open Lwt

let _b2b u = 
  let b = Buffer.create 1024 in
  let () = Update.to_buffer b u in
  let flat = Buffer.contents b in
  let u',_ = Update.from_buffer flat 0 in
  u'

let _cmp = OUnit.assert_equal ~printer:Update.update2s

let test_sequence () =
  let s = Update.Sequence [
    Update.make_master_set "Zen" None;
    Update.Set ("key", "value");
    Update.Delete "key";
    Update.TestAndSet ("key",None, Some "X")
  ] in
  let s' = _b2b s in
  _cmp s s'

let test_interval() = 
  let r0 = Interval.max in
  let u0 = Update.SetInterval r0 in
  let u0' = _b2b u0 in
  _cmp u0 u0';
  let r1 = Interval.make (Some "b") (Some "k") (Some "a") (Some "z") in
  let u1 = Update.SetInterval r1 in
  let u1' = _b2b u1 in
  _cmp u1 u1'


let test_interval2() = 
  let t () = 
    let i0 = Interval.make (Some "a") (Some "b") None (Some "c") in
    let fn = "/tmp/test_interval2.bin" in
    Lwt_io.with_file ~mode:Lwt_io.output fn
    (fun oc -> Interval.output_interval oc i0) 
    >>= fun () ->
    Lwt_io.with_file ~mode:Lwt_io.input fn (fun ic ->
      Interval.input_interval ic >>= fun i1 ->
      OUnit.assert_equal ~printer:Interval.to_string i0 i1;
      Lwt.return ())
  in
  Lwt_extra.run (t())

let test_delete_prefix () =
  let u = Update.DeletePrefix "whatever" in
  let u' = _b2b u in
  _cmp u u' 

let test_assert ()=
  let _ = Update.Set ("keyi", "valuei") in
  let u = Update.Assert ("key", Some "value") in
  let u' = _b2b u in
  _cmp u u'

let test_assert_exists () =
  let _= Update.Set ("keyi", "valuei") in
  let u = Update.Assert_exists ("keyi") in
  let u' = _b2b u in
  _cmp u u'
(*
  let u = Update.Set ("key2", "value")in
  let u2 = Update.Assert_exists ("mkey")in
  let u3 = Lwt_io.printlf "ok!" in
  let u' = _b2b u2 in
  _cmp u u'
*)

let suite = "update" >:::[
  "sequence" >:: test_sequence;
  "interval" >:: test_interval;
  "interval2">:: test_interval2;
  "delete_prefix" >:: test_delete_prefix;
  "assert_exists" >:: test_assert_exists;
  "assert"        >:: test_assert;
]
