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

module Message : sig
  type t =
    | Immediate1_exn of string * exn option
    | ImmediateN of string list
    | Delayed1_exn of (unit -> string) * exn option

  val traverse_ : (string -> exn option -> unit Lwt.t) -> t -> unit Lwt.t
  val length : t -> int
end = struct
  type t =
    | Immediate1_exn of string * exn option
    | ImmediateN of string list
    | Delayed1_exn of (unit -> string) * exn option

  let traverse_ f = function
    | Immediate1_exn (s, e) -> f s e
    | ImmediateN msgs -> Lwt_list.iter_s (fun m -> f m None) msgs
    | Delayed1_exn (m, e) -> f (m ()) e

  let length = function
    | Immediate1_exn _ -> 1
    | ImmediateN msgs -> List.length msgs
    | Delayed1_exn _ -> 1
end

module Entry = struct
  type t = {
    time : float;
    level : Lwt_log.level;
    payload : Message.t;
    section : Lwt_log.section;
  }
end

module R = Ring_buffer.Make(struct
    type t = Entry.t
    let zero = Entry.({
        time = 0.0;
        level = Lwt_log.Debug;
        payload = Message.ImmediateN [];
        section = Lwt_log.Section.main;
    })
end)

let max_crash_log_size = 1000
let circ_buf = R.create ~size:max_crash_log_size

let add_to_crash_log section level msgs =
  let entry = Entry.({
    time = Unix.time ();
    level;
    payload = msgs;
    section;
  }) in
  R.insert entry circ_buf

let dump_crash_log crash_log_sink =
  let string_of_level = function
    | Lwt_log.Debug -> "debug"
    | Lwt_log.Info -> "info"
    | Lwt_log.Notice -> "notice"
    | Lwt_log.Warning -> "warning"
    | Lwt_log.Error -> "error"
    | Lwt_log.Fatal -> "fatal"
  in

  let format_message e msg exn = Entry.(
      Printf.sprintf "%Ld: %s %s: %s"
                     (Int64.of_float e.time)
                     (Lwt_log.Section.name e.section)
                     (string_of_level e.level)
                     (match exn with
                      | None -> msg
                      | Some e -> msg ^ Printexc.to_string e))
  in

  let log_entry oc e =
    Message.traverse_ (fun m exn ->
                       Lwt_io.write_line oc (format_message e m exn))
                      e.Entry.payload
  in

  let log_buffer oc =
    let go = R.fold ~f:(fun acc e -> fun () ->
                                     acc () >>= fun () ->
                                     log_entry oc e)
                    ~acc:(fun () -> Lwt.return_unit)
                    circ_buf
    in
    go ()
  in

  let open Arakoon_log_sink in
  match crash_log_sink with
  | File file_name ->
     Lwt_io.with_file
       ~mode:Lwt_io.output (Printf.sprintf "%s.debug.%f" file_name (Unix.time ()))
       (fun oc -> log_buffer oc)
  | Redis (host, port, key) ->
     let key = Printf.sprintf "%s.debug.%f" key (Unix.time ()) in
     let module Re = Redis_lwt.Client in
     Re.connect Re.({ host ;port; }) >>= fun client ->
     R.fold
       ~f:(fun acc e -> fun () ->
                        acc () >>= fun () ->
                        Message.traverse_
                          (fun m exn ->
                           Re.rpush client key [format_message e m exn] >>= fun _list_length ->
                           Lwt.return_unit)
                          e.Entry.payload)
       ~acc:(fun () -> Lwt.return_unit)
       circ_buf
       ()
  | Console ->
     log_buffer Lwt_io.stdout
