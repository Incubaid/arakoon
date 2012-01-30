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
  type 'a t = {
    empty_m: Lwt_mutex.t;
    full_m: Lwt_mutex.t;
    empty: unit Lwt_condition.t;
    full : unit Lwt_condition.t;
    q: 'a Queue.t;
    capacity : int option;
    leaky: bool;
  }

  let create ?(capacity=None) ?(leaky=false)() = {
    empty_m = Lwt_mutex.create();
    full_m = Lwt_mutex.create();
    empty = Lwt_condition.create();
    full  = Lwt_condition.create();
    q = Queue.create();
    capacity = capacity;
    leaky = leaky;
  }
  let create_fixed_capacity i = 
    let capacity = Some i in
    create ~capacity ()

  let _is_full t =
    match t.capacity with
      | None -> false
      | Some c -> Queue.length t.q = c

  let add e t =
    Lwt_mutex.with_lock t.full_m (fun () ->
      let _add e = 
	let () = Queue.add e t.q in
	let () = Lwt_condition.signal t.empty () in
	Lwt.return () 
      in
      if _is_full t 
      then 
	if t.leaky
	then Lwt_log.debug "leaky buffer reached capacity: dropping"
	else
	  begin
	    Lwt_condition.wait ~mutex:t.full_m t.full >>= fun () ->
	    _add e
	  end
      else 
	_add e
    ) 
      
  let take t =
    Lwt_mutex.with_lock t.empty_m (fun () ->
      if Queue.is_empty t.q
      then Lwt_condition.wait ~mutex:t.empty_m t.empty
      else Lwt.return ()
    ) >>= fun () ->
    let e = Queue.take t.q in
    let () = Lwt_condition.signal t.full () in
    Lwt.return e

  let wait_for_item t =
    Lwt_mutex.with_lock t.empty_m (fun () ->
      if Queue.is_empty t.q then
	Lwt_condition.wait t.empty
      else Lwt.return ()
    )

  let wait_until_empty t = 
    if Queue.is_empty t.q 
    then Lwt.return ()
    else Lwt_condition.wait ~mutex:t.full_m t.full 
end
