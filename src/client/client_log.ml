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

let debug_void _x = Lwt.return ()

let debug_f_void fmt =
  (* No iksprintf, and no obvious way to emulate it *)
  Printf.ksprintf (fun _ -> Lwt.return ()) fmt

let lwt_log_enabled = ref false

let enable_lwt_logging_for_client_lib_code () =
  lwt_log_enabled := true

let debug x =
  if !lwt_log_enabled then Lwt_log.debug x else debug_void x

let debug_f x =
  if !lwt_log_enabled then Lwt_log.debug_f x else debug_f_void x
