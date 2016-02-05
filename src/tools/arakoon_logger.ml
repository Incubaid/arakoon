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


let setup_log_sinks log_sinks =
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
  Lwt.return ()
