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

let section = Logger.Section.main

let backoff ?(min=0.125) ?(max=8.0) (f:unit -> 'a Lwt.t) =
  let rec loop t =
    Lwt.catch
      (fun () -> Lwt.pick [Lwt_unix.timeout t;f () ] )
      (function
        | Lwt_unix.Timeout ->
          begin
            let t' = t *. 2.0 in
            if t' < max then
              Logger.info_f_ "retrying with timeout of %f" t' >>= fun () ->
              loop t'
            else Lwt.fail (Failure "max timeout exceeded")
          end
        | x -> Lwt.fail x)
  in
  loop min >>= fun result ->
  Lwt.return result
