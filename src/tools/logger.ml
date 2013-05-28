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
    match exn with
      | Some exn -> Lwt_log.log ~exn ~section ~level msg
      | None -> Lwt_log.log ~section ~level msg

let log_ ?exn section level dmsg =
  if level < Lwt_log.Section.level section
  then
    Crash_logger.add_to_crash_log section level [Crash_logger.Delayed (dmsg, exn)]
  else
    match exn with
      | Some exn -> Lwt_log.log ~exn ~section ~level (dmsg ())
      | None -> Lwt_log.log ~section ~level (dmsg ())

