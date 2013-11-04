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

let _compress_tlog
      ~compress
      ?(cancel=(ref false)) tlog_name archive_name =
  let limit = 2 * 1024 * 1024 (* 896 * 1024  *)in
  let buffer_size = limit + (64 * 1024) in
  Lwt_io.with_file ~mode:Lwt_io.input tlog_name
    (fun ic ->
       Lwt_io.with_file ~mode:Lwt_io.output archive_name
         (fun oc ->
            let rec fill_buffer (buffer:Buffer.t) (last_i:Sn.t) (counter:int) =
              Lwt.catch
                (fun () ->
                   Tlogcommon.read_entry ic >>= fun (entry:Entry.t) ->
                   let i = Entry.i_of entry
                   and v = Entry.v_of entry
                   in
                   Tlogcommon.entry_to buffer i v;
                   let (last_i':Sn.t) = if i > last_i then i else last_i in
                   if Buffer.length buffer < limit || counter = 0
                   then fill_buffer (buffer:Buffer.t) last_i' (counter+1)
                   else Lwt.return (last_i',counter)
                )
                (function
                  | End_of_file -> Lwt.return (last_i,counter)
                  | exn -> Lwt.fail exn
                )
            in
            let compress_and_write last_i buffer =
              let contents = Buffer.contents buffer in
              let t0 = Unix.gettimeofday () in
              Lwt_preemptive.detach (fun () -> compress contents) ()
              >>= fun output ->
              begin
                if !cancel
                then
                  Lwt.fail Canceled
                else
                  Lwt.return ()
              end >>= fun () ->
              let t1 = Unix.gettimeofday() in
              let d = t1 -. t0 in
              let cl = String.length contents in
              let ol = String.length output in
              let factor = (float cl) /. (float ol) in
              Logger.debug_f Logger.Section.main "compression: %i bytes into %i (in %f s) (factor=%2f)" cl ol d factor
              >>= fun () ->
              Llio.output_int64 oc last_i >>= fun () ->
              Llio.output_string oc output >>= fun () ->
              let sleep = 2.0 *. d in
              Logger.debug_f Logger.Section.main "compression: sleeping %f" sleep >>= fun () ->
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
      ~uncompress archive_name tlog_name =
  Lwt_io.with_file ~mode:Lwt_io.input archive_name
    (fun ic ->
       Lwt_io.with_file ~mode:Lwt_io.output tlog_name
         (fun oc ->
            let rec loop () =
              Lwt.catch
                (fun () ->
                   Sn.input_sn ic >>= fun last_i ->
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
  let c = match compressor with
  | Bz2 -> compress_bz2
  | Snappy -> compress_snappy
  | No -> failwith "compressor is 'No'"
  in _compress_tlog ~compress:c ~cancel tlog_name archive_name

let uncompress_tlog archive_name tlog_name =
  let len = String.length archive_name in
  let suffix = String.sub archive_name (len - 4) 4
  in
  let u = match suffix with
    | ".tlf" -> uncompress_bz2
    | ".tls" -> uncompress_snappy
    | _ -> failwith (Printf.sprintf "invalid archive name %s" archive_name)
  in
  _uncompress_tlog ~uncompress:u archive_name tlog_name
