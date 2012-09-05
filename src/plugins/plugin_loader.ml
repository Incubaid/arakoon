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
      (try
         Dynlink.loadfile_private qual;
         Lwt.return ()
       with
           (Dynlink.Error e) as x -> let msg = Dynlink.error_message e in
                              Lwt_log.fatal_f "Plugin_loader: [%S] failed with %S" qual msg >>= fun () ->
                              Lwt.fail x
      ) >>= fun () ->
      _inner ps 
      in
  _inner pnames
