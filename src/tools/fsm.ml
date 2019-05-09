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


type 'e e_execute = 'e -> unit Lwt.t

type ('s, 'e) unit_transition  = unit -> ('s * 'e list) Lwt.t
type ('m,'s, 'e) msg_transition = 'm   -> ('s * 'e list) Lwt.t

type ('m, 's, 'e) state =
  | Msg_arg  of  ('m, 's,'e) msg_transition
  | Unit_arg of  ('s, 'e) unit_transition
and  ('m, 's,'e) lookup = 's -> ('m,'s,'e) state


let do_side_effects e_execute es  =
  Lwt_list.iter_s e_execute es

let return ?(sides=[]) x = Lwt.return (x, sides)



let nop_trace _ = Lwt.return ()

let loop ?(trace=nop_trace)
      ?(stop=ref false)
      (e_execute : 'e e_execute)
      produce lookup (transition: ('s,'e) unit_transition) =
  let rec _interprete key =
    if !stop
    then
      Logger.debug ~section:Logger.Section.main "Stopping fsm"
    else
      begin
        let arg, product_type = lookup key in
        match arg with
          | Unit_arg next -> _step_unit next
          | Msg_arg next -> produce product_type >>= fun msg ->
                            _step_msg next msg
      end
  and _step_unit transition =
    transition () >>= fun (key, es) ->
    do_side_effects e_execute es >>= fun () ->
    trace key >>= fun () ->
    _interprete key
  and _step_msg transition msg =
    transition msg >>= fun (key, es) ->
    do_side_effects e_execute es >>= fun () ->
    trace key >>= fun () ->
    _interprete key
  in _step_unit transition

let expect_loop
      (e_execute : 'e e_execute)
      expected step_count trans_init produce lookup (transition: ('s,'e) unit_transition) =
  let rec _step_unit prev_key step_count transition =
    if step_count = 0 then Lwt.fail (Failure "out of steps!")
    else
      begin
        transition () >>= fun (key, es) ->
        do_side_effects e_execute es >>= fun () ->
        expected prev_key key >>= function
        | Some x ->
          (* Printf.printf "XXX finished\n"; *)
          Lwt.return x
        | None ->
          (* Printf.printf "XXX continuing %d\n" step_count; *)
          let arg,product_type = lookup key in
          match arg with
            | Unit_arg next -> _step_unit key (step_count-1) next
            | Msg_arg next ->
              produce product_type >>= fun msg ->
              _step_msg key (step_count-1) next msg
      end
  and _step_msg prev_key step_count transition msg =
    if step_count = 0 then Lwt.fail (Failure "out of steps!")
    else
      begin
        transition msg >>= fun (key, es) ->
        do_side_effects e_execute es >>= fun () ->
        expected prev_key key >>= function
        | Some x ->
          (* Printf.printf "XXX finished\n"; *)
          Lwt.return x
        | None ->
          (* Printf.printf "XXX continuing %d\n" step_count; *)
          let arg,product_type = lookup key in
          match arg with
            | Unit_arg next -> _step_unit key (step_count-1) next
            | Msg_arg next ->
              produce product_type >>= fun msg ->
              _step_msg key (step_count-1) next msg
      end
  in _step_unit trans_init step_count transition
