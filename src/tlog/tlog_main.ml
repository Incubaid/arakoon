open Tlogcommon
open Lwt

let dump_tlog filename ~values=
  let printer () entry =
    let i = Entry.i_of entry 
    and v = Entry.v_of entry
    and m = Entry.m_of entry in
    let ms = match m with None -> "" | Some s -> Printf.sprintf ":%S" s
    in
    Lwt_io.printlf "%s:%s%s" (Sn.string_of i) 
      (Value.value2s v ~values) ms
  in
  let folder,_,index = Tlc2.folder_for filename None in
  
  let t =
    begin
	  let do_it ic =
        
	    let lowerI = Sn.start
	    and higherI = None
	    and first = Sn.of_int 0
	    and a0 = () in
	    folder ic ~index lowerI higherI ~first a0 printer >>= fun () ->
	    Lwt.return 0
	  in
	  Lwt_io.with_file ~mode:Lwt_io.input filename do_it
    end
  in
  Lwt_extra.run t

let _last_entry filename = 
  let last = ref None in
  let printer () entry = 
    let i = Entry.i_of entry in
    let p = Entry.p_of entry in
    let () = last := Some entry in
    Lwt_io.printlf "%s:%Li" (Sn.string_of i) p
  in
  let folder,_,index = Tlc2.folder_for filename None in
  let do_it ic = 
    let lowerI = Sn.start in
    let higherI = None 
    and first = Sn.of_int 0
    and a0 = () in
    folder ic ~index lowerI higherI ~first a0 printer 
  in
  Lwt_io.with_file ~mode:Lwt_io.input filename do_it >>= fun () ->
  Lwt.return !last
      

let strip_tlog filename = 
  let maybe_truncate = 
    function 
      | None -> Lwt.return 0
      | Some e -> 
          if Entry.has_marker e 
          then
            begin
              let p = Entry.p_of e in
              let pi = Int64.to_int p in
              Lwt_io.printlf "last=%s => truncating to %i" (Entry.entry2s e) pi >>= fun () ->
              Lwt_unix.truncate filename pi >>= fun () ->
              Lwt.return 0
            end
          else
            begin
              Lwt_io.eprintlf "no marker found, not truncating" >>= fun () ->
              Lwt.return 1
            end
  in
  let t =
    _last_entry filename >>= fun last ->
    maybe_truncate last 
  in
  Lwt_extra.run t

let mark_tlog file_name node_name = 
  let t = 
    _last_entry file_name >>= fun last ->
    match last with
      | None -> Lwt.return 0
      | Some e ->
          let i     = Entry.i_of e 
          and value = Entry.v_of e 
          in
          let f oc = Tlogcommon.write_marker oc i value (Some node_name) in
          Lwt_io.with_file 
            ~mode:Lwt_io.output 
            ~flags:[Unix.O_APPEND;Unix.O_WRONLY]
            file_name f
            >>= fun () -> Lwt.return 0
  in
  Lwt_extra.run t

let make_tlog tlog_name (i:int) =
  let sni = Sn.of_int i in
  let t =
    let f oc = Tlogcommon.write_entry oc sni 
      (Value.create_client_value [Update.Update.Nop] false)
    in
    Lwt_io.with_file ~mode:Lwt_io.output tlog_name f
  in
  Lwt_extra.run t;0


let compress_tlog tlu =
  let failwith x =
    Printf.ksprintf failwith x in
  let () = if not (Sys.file_exists tlu) then failwith "Input file %s does not exist" tlu in
  let tlf = Tlc2.to_archive_name tlu in
  let () = if Sys.file_exists tlf then failwith "Can't compress %s as %s already exists" tlu tlf in
  let t = 
    let tmp = tlf ^ ".tmp" in
    Compression.compress_tlog tlu tmp >>= fun () ->
    File_system.rename tmp tlf >>= fun () ->
    File_system.unlink tlu
  in
  Lwt_extra.run t;
  0

let uncompress_tlog tlx =
  let t =
    let extension = Tlc2.extension_of tlx in
    if extension = Tlc2.archive_extension then
      begin
	let tlu = Tlc2.to_tlog_name tlx in
	Compression.uncompress_tlog tlx tlu >>= fun () ->
	Unix.unlink tlx;
	Lwt.return ()
      end
    else if extension = ".tlc" then
      begin
	let tlu = Tlc2.to_tlog_name tlx in
	Tlc_compression.tlc2tlog tlx tlu >>= fun () ->
	Unix.unlink tlx;
	Lwt.return ()
      end
    else Lwt.fail (Failure "unknown file format")
  in
  Lwt_extra.run t;0


