(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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

  let is_full t =
    match t.capacity with
      | None -> false
      | Some c -> Queue.length t.q = c

  let add e t =
    Lwt_mutex.with_lock t.full_m
      (fun () ->
         let _add e =
           let () = Queue.add e t.q in
           let () = Lwt_condition.signal t.empty () in
           Lwt.return ()
         in
         if is_full t
         then
           if t.leaky
           then Lwt.return () (* Logger.debug "leaky buffer reached capacity: dropping" *)
           else
             begin
               Lwt_condition.wait (* ~mutex:t.full_m *) t.full >>= fun () ->
               _add e
             end
         else
           _add e
      )

  let take t =
    Lwt_mutex.with_lock t.empty_m
      (fun () ->
         if Queue.is_empty t.q
         then Lwt_condition.wait (* ~mutex:t.empty_m *) t.empty
         else Lwt.return ()
      ) >>= fun () ->
    let e = Queue.take t.q in
    let () = Lwt_condition.signal t.full () in
    Lwt.return e

  let harvest t =
    Lwt_mutex.with_lock t.empty_m
      (fun () -> if Queue.is_empty t.q
        then Lwt_condition.wait (* ~mutex:t.empty_m *) t.empty
        else Lwt.return ()
      ) >>= fun () ->
    let size = Queue.length t.q in
    let rec loop es = function
      | 0 -> List.rev es
      | i ->
        let es' = Queue.take t.q :: es
        and i' = i - 1 in
        loop es' i'
    in
    let es = loop [] size in
    let () = Lwt_condition.signal t.full() in
    Lwt.return es

  let wait_for_item t =
    Lwt_mutex.with_lock t.empty_m
      (fun () ->
         if Queue.is_empty t.q then
           Lwt_condition.wait t.empty
         else Lwt.return ()
      )

  let has_item t =
    not (Queue.is_empty t.q)

  let rec wait_until_empty t =
    if Queue.is_empty t.q
    then Lwt.return ()
    else
      begin
        Lwt_condition.wait (*~mutex:t.full_m *) t.full >>= fun () ->
        wait_until_empty t
      end
end
