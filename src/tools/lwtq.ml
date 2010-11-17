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

module LWTQ = struct
  type 'a t = {
    m: Lwt_mutex.t;
    c: unit Lwt_condition.t;
    q: 'a Queue.t;
  }

  let create () = {
    m = Lwt_mutex.create();
    c = Lwt_condition.create();
    q = Queue.create();
  }

  let add e t =
    Lwt_mutex.with_lock t.m (fun () ->
      let () = Queue.add e t.q in
      Lwt.return ()
    ) >>= fun () ->
    let () = Lwt_condition.signal t.c () in
    Lwt.return ()

  let _take t () =
    begin
      if Queue.is_empty t.q
      then Lwt_condition.wait ~mutex:t.m t.c
      else Lwt.return ()
    end
    >>= fun () ->
    let e = Queue.take t.q in
      return e

  let take t = (* blocked if empty *)
    Lwt_mutex.with_lock t.m (_take t)


(*  let peek t =
    let r =
      if Queue.is_empty t.q
      then None
      else Some (Queue.peek t.q)
    in Lwt.return r
*)

  let length t = Queue.length t.q
end
