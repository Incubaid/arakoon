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

let archive_name tlog_name = tlog_name ^ ".tlc"

let tlog_name archive_name =
  let len = String.length archive_name in
  let ext = String.sub archive_name (len-4) 4 in
  assert (ext=".tlc");
  String.sub archive_name 0 (len-4)


let tlog2tlc tlog_name archive_name =
  let limit = 1024 * 1024 in
  Lwt_io.with_file ~mode:Lwt_io.input tlog_name
    (fun ic ->
       Lwt_io.with_file ~mode:Lwt_io.output archive_name
         (fun oc ->
            let rec fill_buffer buffer i =
              Lwt.catch
                (fun () ->
                   Tlogcommon.read_into ic buffer >>= fun () ->
                   if Buffer.length buffer < limit then
                     fill_buffer buffer (i+1)
                   else
                     Lwt.return i
                )
                (function
                  | End_of_file -> Lwt.return i
                  | exn -> Lwt.fail exn
                )
            in
            let compress_and_write n_entries buffer =
              let contents = Buffer.contents buffer in
              let output = Bz2.compress ~block:9 contents 0 (String.length contents) in
              Llio.output_int oc n_entries >>= fun () ->
              Llio.output_string oc output
            in
            let buffer = Buffer.create limit in
            let rec loop () =
              fill_buffer buffer 0 >>= fun n_entries ->
              if n_entries = 0
              then Lwt.return ()
              else
                begin
                  compress_and_write n_entries buffer >>= fun () ->
                  let () = Buffer.clear buffer in
                  loop ()
                end
            in
            loop ()
         )
    )

let tlc2tlog archive_name tlog_name =
  Lwt_io.with_file ~mode:Lwt_io.input archive_name
    (fun ic ->
       Lwt_io.with_file ~mode:Lwt_io.output tlog_name
         (fun oc ->
            let rec loop () =
              Lwt.catch
                (fun () ->
                   Llio.input_int ic >>= fun n_entries ->
                   Llio.input_string ic >>= fun compressed ->
                   Lwt.return (Some compressed))
                (function
                  | End_of_file -> Lwt.return None
                  | exn -> Lwt.fail exn
                )
              >>= function
              | None -> Lwt.return ()
              | Some compressed ->
                begin
                  let lc = String.length compressed in
                  let output = Bz2.uncompress compressed 0 lc in
                  let lo = String.length output in
                  Lwt_io.write_from_exactly oc output 0 lo >>= fun () ->
                  loop ()
                end
            in loop ()))
