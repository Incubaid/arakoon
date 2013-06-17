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

let eq_string str i1 i2 =
  let msg = Printf.sprintf "%s expected:%s actual:%s" str i1 i2 in
    OUnit.assert_equal ~msg i1 i2

let eq_int str i1 i2 =
  let msg = Printf.sprintf "%s expected:%d actual:%d" str i1 i2 in
    OUnit.assert_equal ~msg i1 i2

let eq_conv conv str i1 i2 =
  let c1 = conv i1 and c2 = conv i2 in
  let msg = Printf.sprintf "%s expected:%s actual:%s" str c1 c2 in
    OUnit.assert_equal ~msg i1 i2

open Lwt

let lwt_bracket setup testcase teardown () =
  let try_lwt_ f =
    Lwt.catch f (fun exn -> Lwt.fail exn)
  in
  Lwt_extra.run
    begin
    try_lwt_ setup >>= fun x ->
    try_lwt_ (fun () ->
      Lwt.finalize (fun () -> testcase x)
	(fun () -> teardown x)
    ) >>= fun () ->
    Lwt.return ()
    end

let lwt_test_wrap testcase =
  let setup = Lwt.return and teardown _ = Lwt.return () in
  lwt_bracket setup testcase teardown

let timeout_thread timeout_sec f =
  let sleep_sec = float_of_int (timeout_sec) in
  let t =
    begin
      Lwt_unix.sleep sleep_sec >>= fun () ->
      f ()
    end in
  let () = Lwt_extra.ignore_result t in
  t
