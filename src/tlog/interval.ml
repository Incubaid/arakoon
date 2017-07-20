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



module Interval = struct
  type t = { pu_b : string option;
             pu_e : string option;
             pr_b : string option;
             pr_e : string option;
           }
  (* (string option * string option) * (string option * string option) *)

  let make pu_b pu_e pr_b pr_e = { pu_b;pu_e;pr_b;pr_e}

  let max = { pu_b = None;
              pu_e = None;
              pr_b = None;
              pr_e = None}

  let to_string t =
    let so2s = function
      | None -> "_"
      | Some s -> s
    in
    Printf.sprintf "{(%s,%s),(%s,%s)}"
      (so2s t.pu_b) (so2s t.pu_e) (so2s t.pr_b) (so2s t.pr_e)

  let is_ok t key =
    match t.pu_b,t.pu_e with
      | None  , None   -> true
      | Some b, None   -> b <= key
      | None  , Some e -> key < e
      | Some b, Some e  -> b <= key && key < e

  let interval_to buf t =
    let so2 buf x= Llio.string_option_to buf x in
    so2 buf t.pu_b;
    so2 buf t.pu_e;
    so2 buf t.pr_b;
    so2 buf t.pr_e

  let serialized_size t =
    let sos = Llio.string_option_ssize in
    sos t.pu_b + sos t.pu_e + sos t.pr_b + sos t.pr_e

  let interval_from b =
    let sof () = Llio.string_option_from b in
    let pu_b = sof () in
    let pu_e = sof () in
    let pr_b = sof () in
    let pr_e = sof () in
    {pu_b;pu_e;pr_b;pr_e}

  open Lwt
  let output_interval oc t=
    let o_so = Llio.output_string_option oc in
    o_so t.pu_b >>= fun () ->
    o_so t.pu_e >>= fun () ->
    o_so t.pr_b >>= fun () ->
    o_so t.pr_e >>= fun () ->
    Lwt.return ()

  let input_interval ic =
    let i_so () = Llio.input_string_option ic in
    i_so () >>= fun pu_b ->
    i_so () >>= fun pu_e ->
    i_so () >>= fun pr_b ->
    i_so () >>= fun pr_e ->
    let r = (make pu_b pu_e pr_b pr_e) in
    Lwt.return r
end
