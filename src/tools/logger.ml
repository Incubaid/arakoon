open Lwt

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
  if level < Lwt_log.Section.level section
  then
    Crash_logger.add_to_crash_log section level [Crash_logger.Immediate (msg, exn)]
  else
    Lwt_log.log ?exn ~section ~level msg

let ign_log ?exn section level msg =
  Lwt.ignore_result (log ?exn section level msg)
let log_ ?exn section level dmsg =
  if level < Lwt_log.Section.level section
  then
    Crash_logger.add_to_crash_log section level [Crash_logger.Delayed (dmsg, exn)]
  else
    Lwt_log.log ?exn ~section ~level (dmsg ())

let ign_log_ ?exn section level dmsg =
  Lwt.ignore_result (log_ ?exn section level dmsg)
