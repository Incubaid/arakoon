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


let collapse_remote ip port cluster_id n = 
  let t () = 
    begin
      if n < 1 
      then Lwt.fail (Failure ("n should be >= 1"))
      else Lwt.return ()
    end >>= fun () ->
    let address = Network.make_address ip port in
    let collapse conn =
      Remote_nodestream.make_remote_nodestream cluster_id conn
      >>= fun (client:Remote_nodestream.nodestream) ->
      client # collapse n >>= fun () ->
      Lwt.return 0 
    in
    Lwt.catch
      (fun () -> Lwt_io.with_connection address collapse)
      (fun exn -> Logger.fatal Logger.Section.main ~exn "remote_collapsing_failed" 
	>>= fun () -> Lwt.return (-1)
      )
  in
  Lwt_extra.run (t () )
