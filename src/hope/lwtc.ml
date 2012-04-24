type ic = Lwt_io.input_channel
type oc = Lwt_io.output_channel

let failfmt fmt = let k x = Lwt.fail (Failure x) in Printf.ksprintf k fmt


open Node_cfg
open Lwt

let configure_logging cfg = 
  let log_dir = Node_cfg.log_dir cfg in
  let level_string = Node_cfg.log_level cfg in
  let level =
    match level_string with
      | "info"    -> Lwt_log.Info
      | "notice"  -> Lwt_log.Notice
      | "warning" -> Lwt_log.Warning
      | "error"   -> Lwt_log.Error
      | "fatal"   -> Lwt_log.Fatal
      | _         -> Lwt_log.Debug
  in
  let template = "$(date):$(level):$(message)" in
  let file_name = log_dir ^ "/" ^ Node_cfg.node_name cfg ^ ".log" in
  Lwt_log.file ~template ~mode:`Append ~file_name () >>= fun logger ->
  Lwt_log.default := logger;
  Lwt_log.Section.set_level Lwt_log.Section.main level;
  Lwt.return () 



let log f =  Printf.kprintf Lwt_log.debug f
