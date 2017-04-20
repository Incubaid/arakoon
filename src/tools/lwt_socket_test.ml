(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)



open Lwt
open OUnit

let test_leak () =
  let listening_socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let () = Lwt_unix.setsockopt listening_socket Unix.SO_REUSEADDR true in
  Lwt_unix.bind listening_socket (Unix.ADDR_INET (Unix.inet_addr_any,
                                                  6688))
  >>= fun () ->
  let () = Lwt_unix.listen listening_socket 10 in
  Lwt.join
    [
      begin
        Lwt_unix.accept listening_socket >>= fun (fd,_addr) ->
        Lwt_unix.wait_read fd >>= fun () ->
        Lwt_unix.close fd >>= fun () ->
        Lwt_unix.close listening_socket >>= fun () ->
        Lwt.return ()
      end;
      begin
        Lwt_io.open_connection (Unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1",
                                                6688)) >>= fun (ic,oc) ->
        Lwt.finalize
          (fun () ->
             Lwt.catch
               (fun () ->
                  Lwt_io.write_char oc 'c' >>= fun () ->
                  Lwt_unix.sleep 0.1 >>= fun () ->
                  Lwt_io.write_char oc 'd' >>= fun () ->
                  Lwt.return ())
               (function
                 | End_of_file -> Lwt.return ()
                 | e -> Lwt.fail e
               )
          )
          (fun () ->
             Lwt_io.close ic >>= fun () ->
             Lwt_io.close oc
             (* supposedly connection completely closed,
                actually shutdown will fail *)
          )
      end
    ]

let wrap t =
  Extra.lwt_bracket
    (fun () -> Lwt.return ())
    t
    (fun () -> Lwt.return ())
let suite = "server_socket" >:::[
    "leak" >:: wrap test_leak
  ]
