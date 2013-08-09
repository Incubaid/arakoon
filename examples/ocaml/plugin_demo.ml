open Lwt
open Arakoon_remote_client
open Arakoon_client

let make_address ip port =
  let ha = Unix.inet_addr_of_string ip in
  Unix.ADDR_INET (ha,port)

let with_client cluster_id (ip,port) f =
  let sa = make_address ip port in
  let do_it connection = 
    make_remote_client cluster_id connection >>= fun client ->
    f client
  in
  Lwt_io.with_connection sa do_it

let find_master cluster_id cfgs =
  let rec loop = function
    | [] -> Lwt.fail (Failure "too many nodes down")
    | cfg :: rest ->
      begin
  let _,(ip, port) = cfg in
  let sa = make_address ip port in
  Lwt.catch
    (fun () ->
      Lwt_io.with_connection sa
        (fun connection ->
    make_remote_client cluster_id connection 
    >>= fun client ->
    client # who_master ()) >>= function
    | None -> Lwt.fail (Failure "No Master")
    | Some m -> Lwt.return m)
    (function 
      | Unix.Unix_error(Unix.ECONNREFUSED,_,_ ) -> loop rest
      | exn -> Lwt.fail exn
    )
      end
  in loop cfgs


let with_master_client cluster_id cfgs f =
  find_master cluster_id cfgs >>= fun master_name ->
  let master_cfg = List.assoc master_name cfgs in
  with_client cluster_id master_cfg f


let vo2s = function
  | None -> "None"
  | Some v -> Printf.sprintf "got %S" v

let plugin_demo (client:Arakoon_client.client) =
  let n = "update_max" in
  client # user_function n (Some "0") >>= fun _ ->
  client # user_function n (Some "23") >>= fun _ ->
  client # user_function n (Some "5") >>= fun vo ->
  Lwt_io.printl (vo2s vo) >>= fun () ->
  Lwt.catch
    (fun () -> client # user_function n (Some "x") 
      >>= fun vo -> Lwt_io.printl (vo2s vo)
    )
    (function e -> 
      let s = Printexc.to_string e in 
      Lwt_io.printlf "oops %s" s
    )


let _ = 
  let cluster_id = "ricky" in
  let cfgs = [
    ("arakoon_0",("127.0.0.1",4000));
    (* 
    ("arakoon_1",("127.0.0.1",4001));
    ("arakoon_2",("127.0.0.1",4002))
    *)
  ]
  in
  Lwt_main.run (with_master_client cluster_id cfgs plugin_demo)
