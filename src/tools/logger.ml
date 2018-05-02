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

module Section =
struct
  let make = Lwt_log.Section.make
  let set_level = Lwt_log.Section.set_level
  let level = Lwt_log.Section.level
  let main = Lwt_log.Section.main
end

type level =
    Lwt_log.level =
  | Debug
  | Info
  | Notice
  | Warning
  | Error
  | Fatal

let log ?exn section level msg =
  Crash_logger.add_to_crash_log section level (Crash_logger.Message.Immediate1_exn (msg, exn));
  if level < Lwt_log.Section.level section
  then Lwt.return_unit
  else Lwt_log.log ?exn ~section ~level msg

let ign_log ?exn section level msg =
  Lwt.ignore_result (log ?exn section level msg)

let log_ ?exn section level dmsg =
  Crash_logger.add_to_crash_log section level (Crash_logger.Message.Delayed1_exn (dmsg, exn));
  if level < Lwt_log.Section.level section
  then Lwt.return_unit
  else Lwt_log.log ?exn ~section ~level (dmsg ())

let ign_log_ ?exn section level dmsg =
  Lwt.ignore_result (log_ ?exn section level dmsg)
