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

open Lwt


let igns = Hashtbl.create 10;;

let () =
  Random.self_init ();
  Lwt.async_exception_hook :=
    (function
      | Lwt.Canceled -> ()
      | exn -> raise exn)

let rec get_key () =
  let candidate = Random.int 1073741823 in
  if Hashtbl.mem igns candidate
  then
    get_key ()
  else
    candidate

let ignore_result (t:'a Lwt.t) =
  let key = get_key () in
  let t' =
    Lwt.finalize
      (fun () -> t)
      (fun () ->
        Hashtbl.remove igns key;
        Lwt.return ()) in
  Hashtbl.add igns key t;
  Lwt.ignore_result t';;



let run t =
  let act =
    Lwt.finalize
      (fun () -> t)
      (fun () ->
        Lwt.catch
          (fun () ->
            let ignored_threads = Hashtbl.fold (fun k t acc -> t :: acc) igns [] in
            Hashtbl.reset igns;
            Lwt.pick (Lwt.return () :: ignored_threads) >>= fun a ->
            Lwt.return ())
          (fun exn -> Lwt.return ()))
  in
  Lwt_main.run act
