type ic = Lwt_io.input_channel
type oc = Lwt_io.output_channel

let failfmt fmt = let k x = Lwt.fail (Failure x) in Printf.ksprintf k fmt


let configure_logging () = 
  let logger = Lwt_log.channel
    ~close_mode:`Keep
    ~channel:Lwt_io.stderr
    ~template:"$(date): $(level): $(message)"
    ()
  in
  Lwt_log.default := logger;
  Lwt_log.Section.set_level Lwt_log.Section.main Lwt_log.Debug

let log f =  Printf.kprintf Lwt_log.debug f
