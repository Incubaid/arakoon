open Lwt
open Common
open Unix.LargeFile
module Clone = struct


    
  let receive_files (ic,oc) ~target_dir = 
    let request f =
      let buf = Buffer.create 32 in
      let () = f buf in
      Lwt_io.write oc (Buffer.contents buf)
      >>= fun () ->
      Lwt_io.flush oc
    in
    let outgoing buf = command_to buf CLONE in
    let incoming ic = 
      Llio.input_int ic >>= fun n_files ->
      Lwt_log.debug_f "n_files:%i" n_files >>= fun ()->
      let rec loop i =
	if i = 0 
	then Lwt.return () 
	else
	  begin
	    Llio.input_string ic >>= fun name ->
	    Llio.input_int64 ic  >>= fun length ->
	    Lwt_log.debug_f "receiving file %s with length %Li" name length >>= fun () ->
	    let file_name = Filename.concat target_dir name in
	    Lwt_io.with_file 
	      ~flags:[Unix.O_CREAT;Unix.O_WRONLY]
	      ~mode:Lwt_io.output
	      file_name (fun oc -> Llio.copy_stream ~length ~ic ~oc) 
	    >>= fun () ->
	    loop (i-1)
	  end
      in
      loop n_files >>= fun () -> 
      Lwt.return () 
    in
    request outgoing >>= fun () ->
    response ic incoming


  let send_files (ic,oc) home_dir =
    Tlc2.get_tlog_names home_dir >>= fun file_names ->
    (* let ok_names = List.filter Tlc2.is_compressed file_names in *)
    let ok_names = file_names in
    let n_files = List.length ok_names in
    Llio.output_int oc 0 >>= fun () ->
    Llio.output_int oc n_files >>= fun () ->
    let rec loop = function
      | []-> Lwt.return ()
      | name :: rest ->
	begin
	  let file_name = Filename.concat home_dir name in
	  let stat = Unix.LargeFile.stat file_name in (*Lwt_unix.stat *)
	  let length = stat.st_size in
	  Lwt_log.debug_f "sending file %s with length %Li" file_name length 
	  >>= fun () ->
	  Llio.output_string oc name >>= fun () ->
	  Llio.output_int64  oc length >>= fun () ->
	  Lwt_io.with_file
	    ~flags:[Unix.O_RDONLY]
	    ~mode:Lwt_io.input
	    file_name (fun ic -> Llio.copy_stream ~length ~ic ~oc)
	  >>= fun () ->
	  loop rest
	end
    in loop ok_names >>= fun () ->
    Lwt.return ()
	
  let clone_node ip port target_dir = 
    let logger = Lwt_log.channel
      ~close_mode:`Keep
      ~channel:Lwt_io.stderr
      ~template:"$(date): $(level): $(message)"
      ()
    in
    Lwt_log.default := logger;
    Lwt_log.Section.set_level Lwt_log.Section.main Lwt_log.Debug;
    let t  = 
      let sa = Network.make_address ip port in
      Lwt_io.with_connection sa (receive_files ~target_dir) >>= fun () ->
      Lwt.return ()
    in
    Lwt_main.run t;
    0
      
end
