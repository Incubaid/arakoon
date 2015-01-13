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

let section = Logger.Section.main

let load home pnames =
  (*
    (* if you really want to turn on the screws,
       and limit what they are allowed to do you can do
       something like this: *)
    let () = Dynlink.allow_only [
               "Pervasives";
               "Registry";
               "Llio";
               "Int32";
             ]
  in
  *)

  let rec _inner = function
    | [] -> Lwt.return ()
    | p :: ps ->
      Logger.info_f_ "loading plugin %s" p >>= fun () ->
      let pwe = p ^ ".cmo" in
      let full = Filename.concat home pwe in
      let qual = Dynlink.adapt_filename full in
      Logger.info_f_ "qualified as: %s" qual >>= fun () ->
      let msg =
        try
          let () = Dynlink.loadfile_private qual in
          None
        with Dynlink.Error e ->
          let em = Dynlink.error_message e in
          Some em
      in
      match msg with
      | None -> _inner ps
      | Some em ->
         let s = Printf.sprintf "%s: Dynlink.Error %S" p em in
         Lwt_log.ign_fatal  s;
         failwith s
  in
  _inner pnames
