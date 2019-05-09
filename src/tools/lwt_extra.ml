(*
 * Copyright 2014 Incubaid BVBA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

let read_txt ic =
  let stream = Lwt_io.read_lines ic in
  let buf = Buffer.create 4096 in
  Lwt_stream.iter
    (fun line ->
     Buffer.add_string buf line;
     Buffer.add_char buf '\n'
    )
    stream >>= fun () ->
  Lwt.return (Buffer.contents buf)

let read_file fn = Lwt_io.with_file ~mode:Lwt_io.Input fn read_txt

let run f =
  Lwt_main.run (
      Lwt.finalize
        f
        (fun () ->
         Lwt_io.flush Lwt_io.stdout >>= fun () ->
         Lwt_io.flush Lwt_io.stderr)
  )
