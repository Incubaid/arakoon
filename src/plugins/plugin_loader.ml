open Lwt

let load home pnames = 
  Lwtc.log "Plugin_loader.load [%s]" (String.concat ";" pnames) >>= fun () ->
  let rec _inner = function
    | [] -> Lwt.return ()
    | p :: ps -> 
      Lwt_log.info_f "Plugin_loader: loading plugin %s" p >>= fun () ->
      let pwe = p ^ ".cmo" in
      let full = Filename.concat home pwe in
      let qual = Dynlink.adapt_filename full in
      Lwt_log.info_f "Plugin_loader: qualified as: %S" qual >>= fun () ->
      Dynlink.loadfile_private qual;
      _inner ps 
      in
  _inner pnames
