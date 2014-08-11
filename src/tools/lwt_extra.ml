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

(** {! Lwt }-based utilities *)

open Lwt

(** Peek inside the content of an {! Lwt_mvar.t }

    Note: this blocks if the {! Lwt_mvar.t } is empty!
 *)
let peek_mvar m =
    Lwt.protected (
        Lwt_mvar.take m >>= fun v ->
        Lwt_mvar.put m v >>= fun () ->
        Lwt.return v)

(** A counting-down latch implementation *)
module CountDownLatch : sig
    type t

    val create : count:int -> t
    val await : t -> unit Lwt.t
    val count_down : t -> unit Lwt.t
end = struct
    (** The latch type *)
    type t = { mvar : int Lwt_mvar.t
             ; condition : unit Lwt_condition.t
             ; mutex : Lwt_mutex.t
             }

    (** Create a new {! CountDownLatch.t }, counting down from the given number *)
    let create ~count =
        let mvar = Lwt_mvar.create count
        and condition = Lwt_condition.create ()
        and mutex = Lwt_mutex.create () in
        { mvar; condition; mutex }

    (** Block until the latch fires, or return immediately if it fired already *)
    let await t =
        Lwt_mutex.with_lock t.mutex (fun () ->
            peek_mvar t.mvar >>= function
              | 0 -> Lwt.return ()
              | _ -> begin
                  let mutex = t.mutex in
                  Lwt_condition.wait ~mutex t.condition
              end)

    (** Decrement the latch by one *)
    let count_down t =
        Lwt_mutex.with_lock t.mutex (fun () ->
            Lwt.protected (
                Lwt_mvar.take t.mvar >>= fun c ->
                let c' = max (c - 1) 0 in
                Lwt_mvar.put t.mvar c' >>= fun () ->
                if c' = 0
                    then Lwt_condition.broadcast t.condition ()
                    else ();
                Lwt.return ()))
end
