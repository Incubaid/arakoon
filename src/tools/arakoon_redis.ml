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
  let stop = ref false in
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
  let close () =
    stop := true;
    Lwt.return_unit
  in
  let redis_t () =
    let module R = Redis_lwt.Client in
    let rec outer () =
      Lwt.catch
        (fun () ->
         R.connect R.({ host ;port; }) >>= fun client ->
         let rec inner () =
           (* TODO push multiple items in one go? *)
           if !stop
           then
             Lwt.fail Lwt.Canceled
           else
             begin
               Lwt_buffer.take buffer >>= fun logline ->
               R.rpush client key logline >>= fun _list_length ->
               inner ()
             end
         in
         inner ())
        (fun _ -> Lwt.return ()) >>= fun () ->
      if !stop
      then Lwt.return ()
      else outer ()
    in
    outer ()
  in
  Lwt.ignore_result (redis_t ());
  lg_output, close
