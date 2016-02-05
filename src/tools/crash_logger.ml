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

let setup_crash_log crash_file_gen =
  let string_of_level = function
    | Lwt_log.Debug -> "debug"
    | Lwt_log.Info -> "info"
    | Lwt_log.Notice -> "notice"
    | Lwt_log.Warning -> "warning"
    | Lwt_log.Error -> "error"
    | Lwt_log.Fatal -> "fatal"
  in

  let dump_crash_log () =
    let log_entry oc e =
      let format_message msg exn = Entry.(
        Printf.sprintf "%Ld: %s %s: %s"
          (Int64.of_float e.time)
          (Lwt_log.Section.name e.section)
          (string_of_level e.level)
          (match exn with
            | None -> msg
            | Some e -> msg ^ Printexc.to_string e))
      in
      Message.traverse_ (fun m e ->
        Lwt_io.write_line oc (format_message m e))
        e.Entry.payload
    in

    let log_buffer oc buffer =
      let go = R.fold ~f:(fun acc e -> fun () ->
        acc () >>= fun () ->
        log_entry oc e)
        ~acc:(fun () -> Lwt.return_unit)
        buffer
      in
      go ()
    in

    let crash_file_path = crash_file_gen () in
    Lwt_io.with_file ~mode:Lwt_io.output crash_file_path
      (fun oc -> log_buffer oc circ_buf)
  in

  (add_to_crash_log, dump_crash_log)

let file_logger file_name =
  let flags = [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_APPEND; Unix.O_NONBLOCK] in
  Lwt_unix.openfile file_name flags 0o640 >>= fun fd ->
  Lwt_unix.set_close_on_exec fd;
  let mutex = Lwt_mutex.create () in
  let lg_output line =
    Lwt_mutex.with_lock
      mutex
      (fun () ->
       let rec inner offset len =
         Lwt_unix.write fd line offset len >>= fun written ->
         if written = len
         then Lwt.return_unit
         else inner (offset + written) (len - written)
       in
       inner 0 (Bytes.length line) >>= fun () ->
       Lwt_unix.write fd "\n" 0 1 >>= fun _ ->
       Lwt.return_unit)
  in
  let close () = Lwt_unix.close fd in
  Lwt.return (lg_output, close)


let setup_log_sinks log_sinks crash_log_sinks =
  Lwt_list.map_p
    (let open Arakoon_log_sink in
     function
     | File file_name ->
        file_logger file_name
     | Redis (host, port, key) ->
        Arakoon_redis.make_redis_logger
          ~host ~port ~key
        |> Lwt.return)
    log_sinks >>= fun loggers ->
  let hostname = Unix.gethostname () in
  let component = "arakoon" in
  let pid = Unix.getpid () in
  let seqnum = ref 0 in
  let logger = Lwt_log.make
                 ~output:(fun section level lines ->
                          let seqnum' = !seqnum in
                          let () = incr seqnum in
                          let ts = Unix.gettimeofday () in
                          let logline =
                            Printf.sprintf
                              "%s - %s - %i/0 - %s - %i - %s - %s"
                              (let open Unix in
                               let tm = gmtime ts in
                               Printf.sprintf
                                 "%04d/%02d/%02d %02d:%02d:%02d %d"
                                 (tm.tm_year + 1900)
                                 (tm.tm_mon + 1)
                                 tm.tm_mday
                                 tm.tm_hour
                                 tm.tm_min
                                 tm.tm_sec
                                 (int_of_float ((mod_float ts 1.) *. 1_000_000.))
                              )
                              hostname
                              pid
                              component
                              seqnum'
                              (Lwt_log.string_of_level level)
                              (String.concat "\n" lines)
                          in

                          Lwt_list.iter_p
                            (fun (lg_output, _) -> lg_output logline)
                            loggers)
                 ~close:(fun () ->
                         Lwt_list.iter_p
                           (fun (_, close) -> close ())
                           loggers)
  in
  Lwt_log.default := logger;
  Lwt.return (fun () ->
              (* TODO this should dump the crash log *)
              Lwt.return_unit)

let setup_default_logger
      file_log_path crash_log_prefix
      (* log_sinks crash_log_sinks *)
  =
  Lwt.catch
    (fun () ->
       Lwt_log.file
         ~template:"$(date) $(milliseconds): ($(section)|$(level)): $(message)"
         ~mode:`Append ~file_name:file_log_path ()
    )
    (fun exn ->
       let msg = Printexc.to_string exn in
       let text = Printf.sprintf "could not create file logger %S : %s" file_log_path msg in
       Lwt.fail (Failure text)
    )
  >>= fun file_logger ->
  let (log_crash_msg, dump_crash_log) =
    setup_crash_log crash_log_prefix in

  let add_log_msg section level msgs =
    let log_file_msg msg = Lwt_log.log
                             ~section
                             ~logger:file_logger
                             ~level msg
    in
    Lwt.catch
      (fun () ->
         Lwt_list.iter_s log_file_msg msgs >>= fun () ->
         log_crash_msg section level (Message.ImmediateN msgs);
         Lwt.return_unit)
      (function
        | Lwt_log.Logger_closed -> Lwt.return ()
        | e -> Lwt.fail e
      )
  in

  let close_default_logger () = Lwt_log.close file_logger in
  let default_logger = Lwt_log.make ~output:add_log_msg ~close:close_default_logger in
  Lwt_log.default := default_logger;
  Lwt.return dump_crash_log
