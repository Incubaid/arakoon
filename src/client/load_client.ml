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

let load_scenario ~tls cfg_name x =
  let f cn =
    Client_main.with_master_client ~tls
      cfg_name
      (fun client ->
         let rec loop i =
           if i = 1000
           then Lwt.return ()
           else
             begin
               Lwt_io.printlf "%04i: %i" cn i >>= fun () ->
               let key = Printf.sprintf "%i:%i_key" cn i in
               client # set key key >>= fun () ->
               Lwt_unix.sleep 0.1   >>= fun () ->
               loop (i + 1)
             end
         in
         loop 0
      )
  in
  let cnis =
    let rec loop  acc = function
      | 0 -> acc
      | i -> loop (i :: acc) (i-1)
    in
    loop [] x
  in
  Lwt_list.iter_p f cnis

let main ~tls cfg_name n = Lwt_main.run (load_scenario ~tls cfg_name n);0
