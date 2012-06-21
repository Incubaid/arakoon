open Lwt
open Routing

let try_fetch name (f:unit -> 'a Lwt.t) (r2s: 'a -> string)  =
  Lwt.catch
    (fun () -> 
      f () >>= fun r ->
      let s = r2s r in 
      Lwt_io.printlf "%s: %s" name s
    )
    (function
      | Not_found -> Lwt_io.printlf "%s : --" name
      | e ->         Lwt_io.printlf "%s : %s" name (Printexc.to_string e)
    )

let _interval2s ((a,b), (c,d)) = 
  let so2s so = Log_extra.string_option2s so in
  Printf.sprintf "(%s,%s) (%s,%s)" (so2s a) (so2s b) (so2s c) (so2s d)

let _dump_routing store =  try_fetch "routing" (store # get_routing) Routing.to_s

let _dump_interval store = try_fetch "interval" (store # get_interval) _interval2s

let summary store =
  store # consensus_i () >>= fun consensus_i ->
  store # who_master ()  >>= fun mdo ->
  Lwt_io.printlf "i: %s" (Log_extra.option2s Sn.string_of consensus_i) >>= fun () ->
    let s = 
      match mdo with
	| None -> "None"
	| Some (m,e) -> Printf.sprintf "Some(%s,%s)" m (Sn.string_of e) 
    in
    Lwt_io.printlf "master: %s" s
  >>= fun () -> 
  _dump_routing store >>= fun () ->
  _dump_interval store

let dump_store filename = 
  let t () = 
    Local_store.make_local_store filename >>= fun store ->
    summary store >>= fun () ->
    store # close () 
  in
  Lwt_main.run (t());
  0
