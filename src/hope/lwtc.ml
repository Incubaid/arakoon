type ic = Lwt_io.input_channel
type oc = Lwt_io.output_channel

let failfmt fmt = let k x = Lwt.fail (Failure x) in Printf.ksprintf k fmt

let log f =  Printf.kprintf Lwt_io.printl f
