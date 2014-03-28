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
  let rec _inner = function
    | [] -> Lwt.return ()
    | p :: ps ->
      Logger.info_f_ "loading plugin %s" p >>= fun () ->
      let pwe = p ^ ".cmo" in
      let full = Filename.concat home pwe in
      let qual = Dynlink.adapt_filename full in
      Logger.info_f_ "qualified as: %s" qual >>= fun () ->
      Dynlink.loadfile_private qual;
      _inner ps
  in
  _inner pnames
