(*
Copyright 2016 iNuron NV

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

open Lwt.Infix

let make_redis_logger ~host ~port ~key =
  let open Lwt_buffer in
  let buffer = Lwt_buffer.create_fixed_capacity 100 in
  let lg_output line =
    if Lwt_buffer.is_full buffer
    then
      (* dropping a log message *)
      Lwt.return_unit
    else
      Lwt_buffer.add
        line
        buffer
  in
  let reopen () = Lwt.return_unit in
  let redis_t () =
    let module R = Redis_lwt.Client in
    let rec outer () =
      Lwt.catch
        (fun () ->
         let rec inner client =
           (* TODO push multiple items in one go? *)
           Lwt_buffer.take buffer >>= fun logline ->
           R.rpush client key [logline] >>= fun _list_length ->
           inner client
         in
         R.with_connection
           R.({ host ;port; })
           inner)
        (fun _ -> Lwt.return ()) >>= fun () ->
      Lwt_unix.sleep 1. >>= fun () ->
      outer ()
    in
    outer ()
  in
  Lwt.ignore_result (redis_t ());
  lg_output, reopen
