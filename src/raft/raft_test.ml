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

open Std
open Raft
open Lwt

class mem_transaction_log = fun () ->
  object (self :# transaction_log)
    val mutable log = []
    method append_entries predecessor term entries =
      match predecessor with
      | None ->
         let app = List.mapi (fun i e -> (Int64.of_int i, term, e)) entries in
         log <- List.rev app;
         (* TODO what if the first append entries request is empty? *)
         let i, _, _ = List.hd log in
         Lwt.return (Success i)
      | Some (prev_index, prev_term) ->
         begin
           let rec inner = function
             | [] ->
                Lwt.return (MisMatch (prev_index, prev_term))
             | (i, t, e) :: tl as log' ->
                if i = prev_index
                then
                  begin
                    if t = prev_term
                    then
                      begin
                        (* ok, append here *)
                        let rec rev_append i log = function
                          | [] ->
                             log
                          | e :: tl ->
                             rev_append (Int64.succ i) ((i, t, e) :: log) tl in
                        log <- rev_append (Int64.succ prev_index) log' entries;
                        let i, _, _ = List.hd log in
                        Lwt.return (Success i)
                      end
                    else
                      (* oops, no match *)
                      Lwt.return (MisMatch (prev_index, prev_term))
                  end
                else
                  inner tl
           in
           inner log
         end

    method get_log () = log
  end

let test_tlog () =
  let tlog = new mem_transaction_log () in

  tlog # append_entries (Some (1L, 1L)) 2L [""] >>= fun success ->
  assert (success = (MisMatch (1L, 1L)));
  assert ((List.length (tlog # get_log ())) = 0);

  tlog # append_entries None 2L ["a";"b";"c"] >>= fun success ->
  assert (success = (Success 2L));
  assert ((List.length (tlog # get_log ())) = 3);
  assert (["c";"b";"a"] = (List.map (fun (_, _, e) -> e) (tlog # get_log ())));

  tlog # append_entries None 2L ["c";"d";"e"] >>= fun success ->
  assert (success = (Success 2L));
  assert ((List.length (tlog # get_log ())) = 3);
  assert (["e";"d";"c"] = (List.map (fun (_, _, e) -> e) (tlog # get_log ())));

  tlog # append_entries (Some (1L, 2L)) 2L ["f";"g"] >>= fun success ->
  assert (success = (Success 3L));
  assert ((List.length (tlog # get_log ())) = 4);
  assert (["g";"f";"d";"c"] = (List.map (fun (_, _, e) -> e) (tlog # get_log ())));

  Lwt.return ()


let wrap f () = Lwt_main.run (f ())

open OUnit

let suite =
  "raft" >::: [
    "tlog" >:: wrap test_tlog
  ]
