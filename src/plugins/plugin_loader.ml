open Lwt

let section = Logger.Section.main

let load home pnames = 
  let rec _inner = function
    | [] -> Lwt.return ()
    | p :: ps -> 
      Logger.info_f_ "loading plugin %s" p >>= fun () ->
      let pwe = p ^ ".cmo" in
      let full = Filename.concat home pwe in
      let qual = Dynlink.adapt_filename full in
      Logger.info_f_ "qualified as: %s" qual >>= fun () ->
      Dynlink.loadfile_private qual;
      _inner ps 
      in
  _inner pnames
