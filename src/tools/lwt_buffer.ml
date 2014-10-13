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

module Lwt_buffer = struct
  (* Notes:

   - Any action (push/pop/...) on `queue` should be guarded using `lock`
   - Conditions are only broadcasted to, don't rely on `signal`
   - Make sure to broadcast the appropriate condition whenever suitable
   - Always pass the lock when waiting for some condition
   - Don't try to be too clever
  *)

  type 'a t = {
    capacity : int option;
    leaky : bool;

    queue : 'a Queue.t;
    lock : Lwt_mutex.t;

    element_removed : unit Lwt_condition.t;
    element_added : unit Lwt_condition.t;
  }

  let create ?(capacity=None) ?(leaky=false) () = {
    capacity;
    leaky;

    queue = Queue.create ();
    lock = Lwt_mutex.create ();

    element_removed = Lwt_condition.create ();
    element_added = Lwt_condition.create ();
  }

  let create_fixed_capacity i =
    create ~capacity:(Some i) ()

  let wait_for_item t =
    Lwt_mutex.with_lock t.lock (fun () ->
      if not (Queue.is_empty t.queue)
        then Lwt.return ()
        else Lwt_condition.wait ~mutex:t.lock t.element_added)

  let wait_until_empty t =
    let rec loop () =
      if Queue.is_empty t.queue
        then Lwt.return ()
        else
          Lwt_condition.wait ~mutex:t.lock t.element_removed >>= loop
    in
    Lwt_mutex.with_lock t.lock loop

  let take t =
    let rec loop () =
      if not (Queue.is_empty t.queue)
        then begin
          let v = Queue.take t.queue in
          Lwt_condition.broadcast t.element_removed ();
          Lwt.return v
        end
        else
          Lwt_condition.wait ~mutex:t.lock t.element_added >>= loop
    in
    Lwt_mutex.with_lock t.lock loop

  let harvest t =
    let harvest' q =
      let l = Queue.fold (fun l v -> v :: l) [] q in
      Queue.clear q;
      List.rev l
    in

    let rec loop () =
      if not (Queue.is_empty t.queue)
        then begin
          let l = harvest' t.queue in
          Lwt_condition.broadcast t.element_removed ();
          Lwt.return l
        end
        else
          Lwt_condition.wait ~mutex:t.lock t.element_added >>= loop
    in
    Lwt_mutex.with_lock t.lock loop

  let add e t =
    let add' () =
      Queue.add e t.queue;
      Lwt_condition.broadcast t.element_added ()
    in

    let go () = match t.capacity with
      | None -> Lwt.return (add' ())
      | Some capacity' -> begin
          let rec loop () =
            if Queue.length t.queue < capacity'
              then Lwt.return (add' ())
              else begin
                if t.leaky
                  then Lwt.return ()
                  else
                    (* Note: Can't simply add when this wait returns, because the
                     * condition is broadcasted, so some other thread could have
                     * added some item already.
                     *)
                    Lwt_condition.wait ~mutex:t.lock t.element_removed >>= loop
              end
          in
          loop ()
      end
    in
    Lwt_mutex.with_lock t.lock go
end
