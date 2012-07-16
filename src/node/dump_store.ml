open Lwt
open Routing
open Node_cfg.Node_cfg

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

let inject_as_head fn node_id cfg_fn = 
  let canonical =
	if cfg_fn.[0] = '/'
	then cfg_fn
	else Filename.concat (Unix.getcwd()) cfg_fn
  in
  let cluster_cfg = read_config canonical in
  let node_cfgs = List.filter 
    (fun ncfg -> node_name ncfg = node_id) 
    cluster_cfg.cfgs 
  in
  let node_cfg = match node_cfgs with 
    | [] -> failwith (Printf.sprintf "unknown node: %S" node_id)
    | x :: _ -> x
  in
  let t () = 
    let tlog_dir = node_cfg.tlog_dir in
    let old_head_name = Filename.concat tlog_dir Tlc2.head_fname  in
    Local_store.make_local_store old_head_name >>= fun old_head ->
    old_head # consensus_i () >>= fun old_head_i ->
    old_head # close ()       >>= fun () ->

    Local_store.make_local_store fn >>= fun new_head ->
    new_head # consensus_i () >>= fun new_head_i ->
    new_head # close ()       >>= fun () ->

    Lwt_io.printlf "# %s @ %s" old_head_name (Log_extra.option2s Sn.string_of old_head_i) >>= fun () ->
    Lwt_io.printlf "# %s @ %s" fn (Log_extra.option2s Sn.string_of new_head_i) >>= fun () ->


    let ok = match old_head_i, new_head_i with
      | None,None -> false
      | Some oi, None -> false
      | None, Some ni -> true
      | Some oi, Some ni -> ni > oi
    in
    Lwt_io.printlf "# head is newer">>= fun () ->
    if not ok then failwith "new head is not an improvement";
    let bottom_n = match new_head_i with
      | None -> failwith "can't happen"
      | Some i -> Sn.to_int (Tlc2.get_file_number i)
    in
    Lwt_io.printf "cp %S %S" fn old_head_name >>=fun () ->
    File_system.copy_file fn old_head_name >>= fun () -> 
    Lwt_io.printlf "# [OK]">>= fun () ->
    Lwt_io.printlf "# remove superfluous .tlf files" >>= fun () ->
    Tlc2.get_tlog_names tlog_dir >>= fun tlns ->
    let old_tlns = List.filter (fun tln -> let n = Tlc2.get_number tln in n < bottom_n) tlns in
    Lwt_list.iter_s 
      (fun old_tln -> 
        let canonical = Filename.concat tlog_dir old_tln in
        Lwt_io.printlf "rm %s" canonical >>= fun () ->
        File_system.unlink canonical
      ) old_tlns >>= fun () ->
    Lwt_io.printlf "# [OK]" >>= fun () ->
    Lwt.return ()
      
  in
  Lwt_main.run (t());
  0
  
