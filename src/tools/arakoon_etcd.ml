open Lwt.Infix

let parse_url url =
  let host,port,path =
    Scanf.sscanf url "%s@:%i/%s" (fun h i p -> h,i,p)
  in
  let peers = [host,port] in
  peers, path

let retrieve_value peers path =
  (*
    curl -X GET "http://127.0.0.1.:5000/v2/keys/path_to/asd/055a61c0_0"
    or
    etcdctl --peers=127.0.0.1:5000 get path_to/asd/055a61c0_0
   *)
  let peers_s = String.concat "," (List.map (fun (h,p) -> Printf.sprintf "%s:%i" h p) peers) in
  let cmd = ["etcdctl";
             "--peers=" ^ peers_s;
             "get";
             path
            ]
  in
  let cmd_s = String.concat " " cmd in
  Lwt_io.eprintlf "cmd_s: %s" cmd_s >>= fun () ->
  let command = Lwt_process.shell cmd_s in
  Lwt_process.with_process_in
    command
    (fun p ->
     let stream = Lwt_io.read_lines (p # stdout) in
     Lwt_stream.to_list stream >>= fun lines ->
     let txt = String.concat "\n" lines in
     Lwt.return txt
    )
