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
open Tlogcommon

type compressor =
  | No
  | Bz2
  | Snappy

let compressor2s = function
  | No -> "No"
  | Bz2 -> "Bz2"
  | Snappy -> "Snappy"

let compress_bz2 b =
  Bz2.compress ~block:9 b 0 (String.length b)

let uncompress_bz2 b =
  let lc = String.length b in
  let output = Bz2.uncompress b 0 lc in
  output

let compress_snappy b = Snappy.compress b
let uncompress_snappy b = Snappy.uncompress b


let format_snappy = "snappy"
let read_format ic = function
  | Snappy -> Llio.input_string ic >>= fun s ->
              if s = format_snappy
              then Lwt.return ()
              else Lwt.fail (Failure "format error")
  | No | Bz2  -> Lwt.return ()


let write_format oc = function
  | Snappy -> Llio.output_string oc format_snappy
  | No | Bz2 -> Lwt.return ()


let _compress_tlog
      ~compressor
      ?(cancel=(ref false)) tlog_name archive_name =
  let limit = 2 * 1024 * 1024 (* 896 * 1024  *)in
  let buffer_size = limit + (64 * 1024) in
  let compress = match compressor with
    | Bz2 -> compress_bz2
    | Snappy -> compress_snappy
    | No -> failwith "compressor is 'No'"
  in
  Lwt_io.with_file
    ~buffer:(Lwt_bytes.create 131072)
    ~mode:Lwt_io.input tlog_name
    (fun ic ->
       let tmp_file = archive_name ^ ".part" in
       Logger.info_f ~section:Logger.Section.main
                     "Compressing %S to %S via %S"
                     tlog_name archive_name tmp_file >>= fun () ->
       File_system.unlink ~verbose:false tmp_file >>= fun () ->
       File_system.with_tmp_file tmp_file archive_name
         (fun oc ->
          write_format oc compressor >>= fun () ->
          let rec fill_buffer (buffer:Buffer.t) (last_i:Sn.t) (counter:int) =
            Lwt.catch
              (fun () -> Tlogcommon.read_entry ic >>= fun entry ->
                         Lwt.return (Some entry)
              )
              (function
                | End_of_file -> Lwt.return None
                | exn -> Lwt.fail exn
              )
            >>= function
            | None -> Lwt.return (last_i,counter)
            | Some entry ->
               begin
                 let i = Entry.i_of entry
                 and v = Entry.v_of entry
                 in
                 Tlogcommon.entry_to buffer i v;
                 let (last_i':Sn.t) = if i > last_i then i else last_i in
                 if Buffer.length buffer < limit || counter = 0
                 then fill_buffer (buffer:Buffer.t) last_i' (counter+1)
                 else Lwt.return (last_i',counter)
               end
          in
          let compress_and_write last_i buffer =
            let contents = Buffer.contents buffer in
            let t0 = Unix.gettimeofday () in
            let output = compress contents in
            begin
              if !cancel
              then Lwt.fail Canceled
              else Lwt.return ()
            end >>= fun () ->
            let t1 = Unix.gettimeofday() in
            let d = t1 -. t0 in
            let cl = String.length contents in
            let ol = String.length output in
            let factor = (float cl) /. (float ol) in
            let section = Logger.Section.main in 
            Logger.debug_f ~section "compression: %i bytes into %i (in %f s) (factor=%2f)" cl ol d factor
            >>= fun () ->
            Llio.output_int64 oc last_i >>= fun () ->
            Llio.output_string oc output >>= fun () ->
            let sleep = 2.0 *. d in
            Logger.debug_f ~section "compression: sleeping %f" sleep >>= fun () ->
            Lwt_unix.sleep sleep
          in
          let buffer = Buffer.create buffer_size in
          let rec loop () =
            fill_buffer buffer (-1L) 0 >>= fun (last_i,counter) ->
            if counter = 0
            then Lwt.return ()
            else
              begin
                compress_and_write last_i buffer >>= fun () ->
                let () = Buffer.clear buffer in
                loop ()
              end
          in
          loop ()
         )
    )

let _uncompress_tlog
      ~compressor archive_name tlog_name =
  let uncompress = match compressor with
    | Snappy -> uncompress_snappy
    | Bz2    -> uncompress_bz2
    | No     -> failwith "No compressor"
  in
  Lwt_io.with_file ~mode:Lwt_io.input archive_name
    (fun ic ->
     read_format ic compressor >>= fun () ->
     Lwt_io.with_file ~mode:Lwt_io.output tlog_name
        (fun oc ->
         let rec loop () =
           Lwt.catch
             (fun () ->
              Sn.input_sn ic >>= fun _last_i ->
              Llio.input_string ic >>= fun compressed ->
              Lwt.return (Some compressed))
             (function
               | End_of_file -> Lwt.return None
               | exn -> Lwt.fail exn
             )
           >>= function
             | None -> Lwt.return ()
             | Some compressed ->
                begin
                  let output = uncompress compressed in
                  let lo = String.length output in
                  Lwt_io.write_from_exactly oc output 0 lo >>= fun () ->
                  loop ()
                end
         in loop ())
    )


let compress_tlog ~cancel tlog_name archive_name compressor=
  _compress_tlog ~compressor ~cancel tlog_name archive_name

let uncompress_tlog archive_name tlog_name =
  let len = String.length archive_name in
  let suffix = String.sub archive_name (len - 4) 4
  in
  let compressor = match suffix with
    | ".tlf" -> Bz2
    | ".tlx" -> Snappy
    | _ -> failwith (Printf.sprintf "invalid archive name %s" archive_name)
  in
  _uncompress_tlog ~compressor archive_name tlog_name
