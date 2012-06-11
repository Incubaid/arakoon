open Lwt

let summary store =
  store # consensus_i () >>= fun consensus_i ->
  store # who_master ()  >>= fun mdo ->
  Lwt_io.printlf "i:%s" (Log_extra.option2s Sn.string_of consensus_i) >>= fun () ->
    let s = 
      match mdo with
	| None -> "None"
	| Some (m,e) -> Printf.sprintf "Some(%s,%s)" m (Sn.string_of e) 
    in
    Lwt_io.printlf "master:%s" s

let dump_store filename = 
  let t () = 
    Local_store.make_local_store filename >>= fun store ->
    summary store >>= fun () ->
    store # close () 
  in
  Lwt_main.run (t());
  0
