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



open OUnit
open Lwt

let test_exception () =
  let too_slow () = Lwt_unix.sleep 3.0 >>= fun () -> Lwt.return "too late" in
  let t =
    Lwt.catch
      (fun () -> Backoff.backoff ~max:1.0 too_slow)
      (function
        | Failure _ -> Lwt.return "ok"
        | x -> Lwt.fail x
      )
  in
  let _ = Lwt_main.run t in ()

let suite = "backoff" >::: [ "exception" >:: test_exception]
