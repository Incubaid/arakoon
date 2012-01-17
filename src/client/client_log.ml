let debug_void _x = Lwt.return ()

let debug_f_void fmt =
  Printf.CamlinternalPr.Tformat.kapr (fun _ -> Obj.magic (fun _ -> Lwt.return ())) fmt
  Lwt.return ()

let lwt_log_enabled = ref false

let enable_lwt_logging_for_client_lib_code () =
  lwt_log_enabled := true

let debug x =
  if !lwt_log_enabled then Lwt_log.debug x else debug_void x

let debug_f x =
  if !lwt_log_enabled then Lwt_log.debug_f x else debug_f_void x
