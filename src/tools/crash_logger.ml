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


type msg =
  | Immediate of string * exn option
  | Delayed of (unit -> string) * exn option

type e = {
  t : float;
  lvl : string;
  msg : msg;
}
let circ_buf = Lwt_sequence.create()
let msg_cnt = ref 0

let add_to_crash_log _section level msgs =
  let max_crash_log_size = 1000 in
  let level_to_string lvl =
    begin
      match lvl with
        | Lwt_log.Debug -> "debug"
        | Lwt_log.Info -> "info"
        | Lwt_log.Notice -> "notice"
        | Lwt_log.Warning -> "warning"
        | Lwt_log.Error -> "error"
        | Lwt_log.Fatal -> "fatal"
    end
  in

  let rec remove_n_elements = function
    | 0  -> ()
    | n ->
      let _ = Lwt_sequence.take_opt_l circ_buf in
      let () = decr msg_cnt in
      remove_n_elements (n-1)
  in

  let add_msg lvl msg  =
    let () = incr msg_cnt in
    let e = {t = Unix.time();lvl = lvl; msg = msg} in
    let _ = Lwt_sequence.add_r e circ_buf  in
    ()
  in
  let new_msg_cnt = List.length msgs in
  let total_msgs = !msg_cnt + new_msg_cnt in
  let () =
    if  total_msgs > 2 * max_crash_log_size then
      begin
        let to_delete = total_msgs - max_crash_log_size in
        (* let () = Printf.printf "removing %i%!\n" to_delete in *)
        remove_n_elements to_delete;
        (* Gc.compact() *)
      end
  in
  let lvls = level_to_string level in
  let () = List.iter (add_msg lvls) msgs  in
  Lwt.return ()


let setup_crash_log crash_file_gen =
  let dump_crash_log () =
    let dump_msgs oc =
      let rec loop () =

        let e = Lwt_sequence.take_l circ_buf in
        let append_exn m exn =
          match exn with
            | None -> m
            | Some exn -> m ^ Printexc.to_string exn in
        let msg =
          Printf.sprintf
            "%Ld: %s: %s"
            (Int64.of_float e.t)
            e.lvl
            (match e.msg with
               | Immediate (m, exn) -> append_exn m exn
               | Delayed (fm, exn) -> append_exn (fm ()) exn) in
        Lwt_io.write_line oc msg >>= fun () ->
        loop ()
      in
      Lwt.catch
        loop
        (function
          | Lwt_sequence.Empty -> Lwt.return ()
          | e -> Lwt.fail e)
    in
    let crash_file_path = crash_file_gen () in
    Lwt_io.with_file ~mode:Lwt_io.output crash_file_path dump_msgs
  in

  let fake_close () = Lwt.return () in

  (add_to_crash_log, fake_close, dump_crash_log)


let setup_default_logger file_log_path crash_log_prefix =
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
  let (log_crash_msg, close_crash_log, dump_crash_log) =
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
         log_crash_msg section level (List.map (fun m -> Immediate (m, None)) msgs) )
      (function
        | Lwt_log.Logger_closed -> Lwt.return ()
        | e -> Lwt.fail e
      )
  in

  let close_default_logger () =
    Lwt_log.close file_logger >>= fun () ->
    close_crash_log ()
  in

  let default_logger = Lwt_log.make add_log_msg close_default_logger in
  Lwt_log.default := default_logger;
  Lwt.return dump_crash_log
