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

open Hotc

module Prefix_otc = struct

  type t = Hotc.db

  let get bdb prefix key =
    Lwt.catch (fun () ->
    let value = Hotc.get bdb (prefix ^ key) in
    (*let _ = log "GET %d" (String.length value) in*)
      Lwt.return value)
      (fun e -> Lwt.fail e)


  let put bdb prefix key value =
    (*let _ = log "PUT %d" (String.length value) in*)
    Lwt.catch (fun () ->
      let () = Hotc.set bdb (prefix ^ key) value in
      Lwt.return ()
    ) (fun e -> Lwt.fail e)

  let out bdb prefix key =
    Lwt.catch (fun () ->
      let () = Hotc.delete_val bdb (prefix ^ key) in
      Lwt.return ()
    ) (fun e -> Lwt.fail e)

  let fold (f:string -> string -> 'c -> 'c) (bdb:t) (prefix:string) (init:'c) =
    Lwt.catch (fun () ->
      let f' a key = f key (Hotc.get bdb key) a in
      let (keys:string array) = Hotc.prefix_keys bdb prefix (-1) in
      let x = Array.fold_left f' init keys in
      Lwt.return x
    ) (fun e -> Lwt.fail e)

  let iter (f:string -> string -> unit) (bdb:t) (prefix:string) =
    fold (fun k v _ -> f k v) bdb prefix ()

  let all_keys bdb prefix =
    fold (fun k v init -> k::init) bdb prefix []

  let all_values bdb prefix =
    fold (fun k v init -> v::init) bdb prefix []

      (* TODO: add more prefixed Otc methods as needed *)

end
