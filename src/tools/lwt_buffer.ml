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
let section = Logger.Section.main

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

  let take_no_wait t =
    if Queue.is_empty t.q
    then None
    else
      let item = Queue.take t.q in
      let () = Lwt_condition.signal t.full () in
      Some item


  let harvest_limited (t:'a t)
                      (item_weight:'a -> int)
                      (max_weight:int) =
    Lwt_mutex.with_lock t.empty_m
      (fun () -> if Queue.is_empty t.q
        then Lwt_condition.wait (* ~mutex:t.empty_m *) t.empty
        else Lwt.return ()
      ) >>= fun () ->
    let size = Queue.length t.q in
    let rec loop es tw = function
      | 0 -> tw, es
      | i ->
         let pot_item = Queue.peek t.q in
         let w = item_weight pot_item in
         let () = Logger.ign_debug_f_ "pot weight =%i" w in
         if tw + w > max_weight
         then tw, es
         else
           let es' = Queue.take t.q :: es
           and i' = i - 1
           and tw' = tw + w in
           loop es' tw' i'
    in
    let tw, es0 = loop [] 0 size in
    let () =
      Logger.ign_debug_f_
        "total weight=%i (max=%i)" tw max_weight
    in
    let es = List.rev es0 in
    assert (es <> []); (* input validation should have rejected this earlier *)
    let () = Lwt_condition.signal t.full() in
    Lwt.return es

  let harvest t =
    let item_weight _ = 0
    and max = 1 in
    harvest_limited t item_weight max


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
