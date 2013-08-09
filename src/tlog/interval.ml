(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
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

  let interval_from s pos =
    let sof s pos = Llio.string_option_from s pos in
    let pu_b,p1 = sof s pos in
    let pu_e,p2 = sof s p1 in
    let pr_b,p3 = sof s p2 in
    let pr_e,p4 = sof s p3 in
    let r = {pu_b;pu_e;pr_b;pr_e} in
    r,p4

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
