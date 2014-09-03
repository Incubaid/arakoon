(*
 * Copyright 2014, Incubaid BVBA
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

module Clock : sig
    type clock = CLOCK_REALTIME
               | CLOCK_REALTIME_COARSE
               | CLOCK_MONOTONIC
               | CLOCK_MONOTONIC_COARSE
               | CLOCK_MONOTONIC_RAW
               | CLOCK_BOOTTIME
               | CLOCK_PROCESS_CPUTIME_ID
               | CLOCK_THREAD_CPUTIME_ID

    type clock_id

    module Timespec : sig
        type t = { sec : int64
                 ; nsec : int64
                 }

        val to_float : t -> float
        val to_string : t -> string
    end

    val get_clock : clock -> clock_id option
    val get_time : clock_id -> Timespec.t
    val get_res : clock_id -> Timespec.t
end = struct
    type clock = CLOCK_REALTIME
               | CLOCK_REALTIME_COARSE
               | CLOCK_MONOTONIC
               | CLOCK_MONOTONIC_COARSE
               | CLOCK_MONOTONIC_RAW
               | CLOCK_BOOTTIME
               | CLOCK_PROCESS_CPUTIME_ID
               | CLOCK_THREAD_CPUTIME_ID

    type clock_id = int

    module Timespec = struct
        type t = { sec : int64
                 ; nsec : int64
                 }

        let to_float t = Int64.to_float t.sec +. (Int64.to_float t.nsec /. 1000000000.)
        let to_string t =
            let open To_string in
            record [ "sec", int64 t.sec
                   ; "nsec", int64 t.nsec
                   ]
    end

    external _arakoon_clocks_getclockid : int -> int option = "arakoon_clocks_getclockid"
    external get_time : clock_id -> Timespec.t = "arakoon_clocks_gettime"
    external get_res : clock_id -> Timespec.t = "arakoon_clocks_getres"

    let get_clock = function
      | CLOCK_REALTIME -> _arakoon_clocks_getclockid 1
      | CLOCK_REALTIME_COARSE -> _arakoon_clocks_getclockid 2
      | CLOCK_MONOTONIC -> _arakoon_clocks_getclockid 3
      | CLOCK_MONOTONIC_COARSE -> _arakoon_clocks_getclockid 4
      | CLOCK_MONOTONIC_RAW -> _arakoon_clocks_getclockid 5
      | CLOCK_BOOTTIME -> _arakoon_clocks_getclockid 6
      | CLOCK_PROCESS_CPUTIME_ID -> _arakoon_clocks_getclockid 7
      | CLOCK_THREAD_CPUTIME_ID -> _arakoon_clocks_getclockid 8
end

include Clock
