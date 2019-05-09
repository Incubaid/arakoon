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
    Lwt.catch
      f
      (fun exn ->
        Lwt_log.fatal ~exn "test failed" >>= fun () ->
        Lwt.fail exn)
  in
  Lwt_main.run
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
