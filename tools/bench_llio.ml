(*
 * Copyright (2010-2014) INCUBAID BVBA
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

open Core.Std
open Core_bench.Std

let (>>=) = Lwt.bind

let run_X_to_bench f b i =
    let cnt = 10 * 1024 in
    let size = b * cnt in

    let buff = Buffer.create size in

    let rec loop = function
      | 0 -> ()
      | n -> f buff i; loop (n - 1)
    in

    loop cnt

let run_output_X_bench f i =
    let cnt = 10 * 1024
    and oc = Lwt_io.null in

    let rec loop = function
      | 0 -> Lwt.return ()
      | n -> f oc i >>= fun () -> loop (n - 1)
    in

    Lwt_main.run (loop cnt)

let run_input_X_bench f =
    let cnt = 10 * 1024
    and ic = Lwt_io.zero in

    let rec loop = function
      | 0 -> Lwt.return ()
      | n -> f ic >>= fun _ -> loop (n - 1)
    in

    Lwt_main.run (loop cnt)

let command =
    let int32_size = 4
    and int32 = 2147483647l
    and int64_size = 8
    and int64 = 9223372036854775807L in
    
    Bench.make_command [
        Bench.Test.create ~name:"Llio_orig.int32_to"
            (fun () -> run_X_to_bench Llio_orig.int32_to int32_size int32);
        Bench.Test.create ~name:"Llio.int32_to"
            (fun () -> run_X_to_bench Llio.int32_to int32_size int32);

        Bench.Test.create ~name:"Llio_orig.int64_to"
            (fun () -> run_X_to_bench Llio_orig.int64_to int64_size int64);
        Bench.Test.create ~name:"Llio.int64_to"
            (fun () -> run_X_to_bench Llio.int64_to int64_size int64);

        Bench.Test.create ~name:"Llio_orig.output_int32"
            (fun () -> run_output_X_bench Llio_orig.output_int32 int32);
        Bench.Test.create ~name:"Llio.output_int32"
            (fun () -> run_output_X_bench Llio.output_int32 int32);

        Bench.Test.create ~name:"Llio_orig.output_int64"
            (fun () -> run_output_X_bench Llio_orig.output_int64 int64);
        Bench.Test.create ~name:"Llio.output_int64"
            (fun () -> run_output_X_bench Llio.output_int64 int64);

        Bench.Test.create ~name:"Llio_orig.input_int32"
            (fun () -> run_input_X_bench Llio_orig.input_int32);
        Bench.Test.create ~name:"Llio.input_int32"
            (fun () -> run_input_X_bench Llio.input_int32);

        Bench.Test.create ~name:"Llio_orig.input_int"
            (fun () -> run_input_X_bench Llio_orig.input_int);
        Bench.Test.create ~name:"Llio.input_int"
            (fun () -> run_input_X_bench Llio.input_int);

        Bench.Test.create ~name:"Llio_orig.input_int64"
            (fun () -> run_input_X_bench Llio_orig.input_int64);
        Bench.Test.create ~name:"Llio.input_int64"
            (fun () -> run_input_X_bench Llio.input_int64);
        ]

let main () = Command.run command
;;

main ()
