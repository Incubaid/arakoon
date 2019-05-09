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

module Section = Lwt_log.Section
type level =
    Lwt_log.level =
  | Debug
  | Info
  | Notice
  | Warning
  | Error
  | Fatal

let debug        = Lwt_log.debug 
let debug_       = Lwt_log.debug
let debug_f      = Lwt_log.debug_f
let debug_f_     = Lwt_log.debug_f
let ign_debug_f_ = Lwt_log.ign_debug_f
let ign_debug_   = Lwt_log.ign_debug_f

let info         = Lwt_log.info
let info_        = Lwt_log.info
let info_f       = Lwt_log.info_f
let info_f_      = Lwt_log.info_f
let ign_info_f_  = Lwt_log.ign_info_f

let warning_f_   = Lwt_log.warning_f
let warning_     = Lwt_log.warning
let ign_warning_ = Lwt_log.ign_warning

let error_       = Lwt_log.error
let error_f_     = Lwt_log.error_f

let fatal_       = Lwt_log.fatal
let fatal        = Lwt_log.fatal
let fatal_f_     = Lwt_log.fatal_f
let ign_fatal_   = Lwt_log.ign_fatal
let ign_fatal_f  = Lwt_log.ign_fatal_f

let log ?exn section level msg =
  Crash_logger.add_to_crash_log section level (Crash_logger.Message.Immediate1_exn (msg, exn));
  if level < Lwt_log.Section.level section
  then Lwt.return_unit
  else Lwt_log.log ?exn ~section ~level msg

let log_ ?exn section level dmsg =
  Crash_logger.add_to_crash_log section level (Crash_logger.Message.Delayed1_exn (dmsg, exn));
  if level < Lwt_log.Section.level section
  then Lwt.return_unit
else Lwt_log.log ?exn ~section ~level (dmsg ())
