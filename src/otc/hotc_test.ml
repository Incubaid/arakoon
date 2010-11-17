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
open Hotc
open Otc
open Prefix_otc
open Extra
open Lwt
open Logging

let eq_string s1 s2 = eq_string "TEST" s1 s2

let test_overal db =
  Hotc.transaction db
    (fun db ->
      let () = Bdb.put db "hello" "world" in
      let x = Bdb.get db "hello" in
      Lwt.return x
    ) >>= fun res ->
  let () = eq_string "world" res in
  Lwt.return ()

let test_with_cursor db =
  Hotc.transaction db
    (fun db' ->
      let () = Bdb.put db' "hello" "world" in
      Hotc.with_cursor db'
	(fun _ cursor ->
	  let () = Bdb.first db' cursor in
	  let x = Bdb.value db' cursor in
	  Lwt.return x
	)
    ) >>= fun res ->
  let () = eq_string "world" res in
  Lwt.return ()

let test_prefix db =
  Hotc.transaction db
    (fun db' ->
      Prefix_otc.put db' "VOL" "hello" "world" >>= fun () ->
      let x = Bdb.get db' "VOLhello" in
      Prefix_otc.get db' "VOL" "hello" >>= fun y ->
      Lwt.return (x,y)
    ) >>= fun (res1, res2) ->
  let () = eq_string "world" res1 in
  let () = eq_string "world" res2 in
  Lwt.return ()

let test_prefix_fold db =
  Hotc.transaction db
    (fun db' ->
      Prefix_otc.put db' "VOL" "hello" "world" >>= fun () ->
      Prefix_otc.put db' "VOL" "hi" "mars"
    ) >>= fun () ->
  Hotc.transaction db
    (fun db' -> Prefix_otc.iter (log "%s %s") db' "VOL")

let test_batch db =
  Hotc.transaction db
    (fun db' ->
       Prefix_otc.put db' "VOL" "ha" "lo" >>= fun () ->
       Prefix_otc.put db' "VOL" "he" "pluto" >>= fun () ->
       Prefix_otc.put db' "VOL" "hello" "world" >>= fun () ->
       Prefix_otc.put db' "VOL" "hi" "mars"
    ) >>= fun () ->
  Hotc.batch db 2 "VOL" None >>= fun batch ->
  match batch with
    | [(k1,s1);(k2,s2)] ->
      begin
      Hotc.batch db 2 "VOL" (Some k2) >>= fun batch2 ->
      match batch2 with
	| [(k3,s3);(k4,s4)] ->
	  eq_string "ha" k1;
	  eq_string "lo" s1;
	  eq_string "he" k2;
	  eq_string "pluto" s2;
	  eq_string "hello" k3;
	  eq_string "world" s3;
	  eq_string "hi" k4;
	  eq_string "mars" s4;
	  Hotc.batch db 2 "VOL" (Some k4) >>= fun batch3 ->
	  assert_equal batch3 [];
	  Lwt.return ()
	| _ -> Lwt.fail (Failure "2:got something else")
      end
    | _ -> Lwt.fail (Failure "1:got something else")

let setup () = Hotc.create "/tmp/foo.tc"

let teardown db =
  Hotc.delete db >>= fun () ->
  let () = Unix.unlink "/tmp/foo.tc" in
  Lwt.return ()

let suite =
  let wrap f = lwt_bracket setup f teardown in
  "Hotc" >:::
    [
      "overal" >:: wrap test_overal;
      "with_cursor" >:: wrap test_with_cursor;
      "prefix" >:: wrap test_prefix;
      "prefix_fold" >:: wrap test_prefix_fold;
      "batch" >:: wrap test_batch;
    ]
