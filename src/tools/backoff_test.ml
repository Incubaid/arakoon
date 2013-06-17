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

open OUnit
open Lwt

let test_exception () =
  let too_slow () = Lwt_unix.sleep 3.0 >>= fun () -> Lwt.return "too late" in
  let t = 
    Lwt.catch 
      (fun () -> Backoff.backoff ~max:1.0 too_slow)
      (function 
   | Failure _ -> Lwt.return "ok"
   | x -> Lwt.fail x
      )
  in
  let _ = Lwt_extra.run t in () 

let suite = "backoff" >::: [ "exception" >:: test_exception]
