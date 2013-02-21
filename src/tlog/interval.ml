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

open Baardskeerder

module Interval = struct
  type t = (string option * string option) * (string option * string option)

  let make pu_b pu_e pr_b pr_e = ((pu_b,pu_e),(pr_b,pr_e))

  let max = ((None,None),(None,None))

  let to_string t =
    let (pu_b,pu_e),(pr_b,pr_e) = t in
    let so2s = function
      | None -> "_"
      | Some s -> s
    in
    Printf.sprintf "(%S,%S),(%S,%S)"
      (so2s pu_b) (so2s pu_e) (so2s pr_b) (so2s pr_e)

  let is_ok t key =
    let ((pu_b,pu_e),(pr_b,pr_e)) = t in
    match pu_b,pu_e with
    | None  , None   -> true
    | Some b, None   -> b <= key
    | None  , Some e -> key < e
    | Some b, Some e  -> b <= key && key < e

  let interval_to buf t=
    let (pu_b,pu_e),(pr_b,pr_e) = t in
    let so2 buf x= Pack.string_option_to buf x in
    so2 buf pu_b;
    so2 buf pu_e;
    so2 buf pr_b;
    so2 buf pr_e

  let interval_from s =
    let sof s = Pack.input_string_option s in
    let pu_b = sof s in
    let pu_e = sof s in
    let pr_b = sof s in
    let pr_e = sof s in
    let r = ((pu_b,pu_e),(pr_b,pr_e)) in
    r

  open Lwt
  let input_interval ic =
    Llio.input_string ic >>= fun s ->
    Lwt_log.debug_f "input_interval :%S" s >>= fun () ->
    let p = Pack.make_input s 4 (* sight *) in
    let iv = interval_from p in
    Lwt.return iv
    
end

