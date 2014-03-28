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



open Lwt

let section = Logger.Section.main

let backoff ?(min=0.125) ?(max=8.0) (f:unit -> 'a Lwt.t) =
  let rec loop t =
    Lwt.catch
      (fun () -> Lwt.pick [Lwt_unix.timeout t;f () ] )
      (function
        | Lwt_unix.Timeout ->
          begin
            let t' = t *. 2.0 in
            if t' < max then
              Logger.info_f_ "retrying with timeout of %f" t' >>= fun () ->
              loop t'
            else Lwt.fail (Failure "max timeout exceeded")
          end
        | x -> Lwt.fail x)
  in
  loop min >>= fun result ->
  Lwt.return result
