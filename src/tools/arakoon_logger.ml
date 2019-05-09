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

let file_logger file_name =
  let flags = [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_APPEND; Unix.O_NONBLOCK] in
  let fd_ref = ref None in
  let get_fd () =
    match !fd_ref with
    | None ->
       Lwt_unix.openfile file_name flags 0o640 >>= fun fd ->
       fd_ref := Some fd;
       Lwt.return fd
    | Some fd ->
       Lwt.return fd
  in
  let mutex = Lwt_mutex.create () in
  let lg_output line =
    Lwt_mutex.with_lock
      mutex
      (fun () ->
       get_fd () >>= fun fd ->
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
  let reopen () =
    Lwt_mutex.with_lock
      mutex
      (fun () ->
       match !fd_ref with
       | None -> Lwt.return_unit
       | Some fd ->
          fd_ref := None;
          Lwt_unix.close fd)
  in
  Lwt.return (lg_output, reopen)

let console_logger =
  let lg_output line =
    Lwt_io.atomic
      (fun oc -> Lwt_io.write_line oc line)
      Lwt_io.stdout
  in
  let reopen () = Lwt.return_unit in
  lg_output, reopen


let format_message
      ~hostname ~pid ~thread_id ~component
      ~(ts:Core.Time.t) ~seqnum
      ~section ~level ~lines
  =
  let _s = section in
  Printf.sprintf
    "%s - %s - %i/%04i - %s - %i - %s - %s"
    (let open Unix in

     let (fts:float) = Core.Time.to_span_since_epoch ts
                       |> Core.Time.Span.to_sec
     in
     let tm = Unix.localtime fts in
     let offset =
       let zone = Lazy.force Core.Time.Zone.local in
       Core.Time.utc_offset ~zone ts
       |> Core.Time.Span.to_parts
     in
     Printf.sprintf
       "%04d-%02d-%02d %02d:%02d:%02d %06d %c%02d%02d"
       (tm.tm_year + 1900)
       (tm.tm_mon + 1)
       tm.tm_mday
       tm.tm_hour
       tm.tm_min
       tm.tm_sec
       (int_of_float ((mod_float fts 1.) *. 1_000_000.))
       (let open Core in
        match offset.Time.Span.Parts.sign with
        | Sign.Neg -> '-'
        | Sign.Pos | Sign.Zero -> '+')
       offset.Core.Time.Span.Parts.hr
       offset.Core.Time.Span.Parts.min
    )
    hostname
    pid thread_id
    component
    seqnum
    (Lwt_log.string_of_level level)
    (String.concat "; " lines)

let reopen_loggers = ref (fun () -> Lwt.return ())

let setup_log_sinks log_sinks =
  Lwt_list.map_p
    (let open Arakoon_log_sink in
     function
     | File file_name ->
        file_logger file_name
     | Redis (host, port, key) ->
        Arakoon_redis.make_redis_logger
          ~host ~port ~key
        |> Lwt.return
     | Console ->
        Lwt.return console_logger)
    log_sinks >>= fun loggers ->
  let hostname = Unix.gethostname () in
  let component = "arakoon" in
  let pid = Unix.getpid () in
  let seqnum = ref 0 in
  let logger = Lwt_log.make
                 ~output:(fun section level lines ->
                          let seqnum' = !seqnum in
                          let () = incr seqnum in
                          let ts = Core.Time.now () in
                          let thread_id = 0 in
                          let logline =
                            format_message
                              ~hostname ~pid ~thread_id ~component
                              ~ts ~seqnum:seqnum'
                              ~section ~level ~lines
                          in

                          Lwt_list.iter_p
                            (fun (lg_output, _) -> lg_output logline)
                            loggers)
                 ~close:(fun () -> Lwt.return ())
  in
  reopen_loggers :=
    (fun () ->
     Lwt_list.iter_p
       (fun (_, reopen) -> reopen ())
       loggers);
  Lwt_log.default := logger;
  Lwt.return ()
