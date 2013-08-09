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

module Entry = struct
  type t = {i: Sn.t ;v : Value.t; p:int64; m: string option}

  let make i v p m : t = {i;v;p;m}
  let i_of t = t.i
  let v_of t = t.v
  let p_of t = t.p
  let m_of t = t.m

  let has_marker t = t.m <> None
  let check_marker t m = t.m = m

  let entry2s t =
    let ms = Log_extra.string_option2s t.m in
    Printf.sprintf "{i=%s;v=%s;m=%s;p=%Li}" (Sn.string_of t.i) (Value.value2s t.v) ms t.p
end

exception TLogCheckSumError of Int64.t
exception TLogUnexpectedEndOfFile of Int64.t

let tlogEntriesPerFile =
  ref (IFDEF SMALLTLOG THEN 1000 ELSE (100 * 1000) END)

let tlogExtension = ".tlog"
let tlogFileRegex =
  let qex = Str.quote tlogExtension in
  Str.regexp ("[0-9]+" ^ qex ^ "$" )



let isValidSuccessor i prevI =
  i = prevI || i = Sn.succ prevI

let calculateTlogEntryChecksum dataToChecksum =
  Int32.of_int (Hashtbl.hash dataToChecksum)

let validateTlogEntry buffer checkSum =
  let calculatedChecksum = calculateTlogEntryChecksum buffer in
  if ( Int32.compare checkSum calculatedChecksum ) <> 0 then
    Llio.lwt_failfmt "Corrupt tlog entry found. Checksum failure. %ld %ld" checkSum calculatedChecksum
  else
    Lwt.return()


let read_entry ic =
  let last_valid_pos = Lwt_io.position ic in
  Lwt.catch
    (fun () ->
      Sn.input_sn    ic >>= fun  i     ->
      Llio.input_int32 ic >>= fun chkSum ->
      Llio.input_string ic >>= fun cmd  ->
      let cmdl = String.length cmd in
      let chksum2 = Crc32c.calculate_crc32c cmd 0 cmdl in
      begin
        if chkSum <> chksum2
        then Lwt.fail (TLogCheckSumError last_valid_pos )
        else Lwt.return ()
      end >>= fun () ->
      let value,off = Value.value_from cmd 0 in
      let marker =
        if off = cmdl
        then None
        else
          let m,_ = Llio.string_option_from cmd off in
          m
      in
      let (entry : Entry.t) = Entry.make i value last_valid_pos marker in
      Lwt.return entry)
    (function
      | End_of_file ->
          begin
            let new_pos = Lwt_io.position ic in
            Logger.log_ Logger.Section.main Logger.Debug
              (fun () ->
                Printf.sprintf "Last valid pos: %d, new pos: %d"
                  (Int64.to_int last_valid_pos) (Int64.to_int new_pos)) >>= fun () ->
            begin
              if ( Int64.compare new_pos last_valid_pos ) = 0
              then Lwt.fail End_of_file
              else
                begin
                  Logger.log Logger.Section.main Logger.Debug "Failing with TLogUnexpectedEndOfFile" >>= fun () ->
                  Lwt.fail (TLogUnexpectedEndOfFile last_valid_pos)
                end
            end
          end
      | ex -> Lwt.fail ex)


let entry_from buff pos =
  let i, pos2  = Sn.sn_from       buff pos  in
  let crc,pos3 = Llio.int32_from  buff pos2 in
  let cmd,pos4 = Llio.string_from buff pos3 in
  let value,_ = Value.value_from cmd 0 in
  let e = Entry.make i value 0L None in
  e, pos4


let read_into ic buf =
  Sn.input_sn ic >>= fun i ->
  Llio.input_int32 ic >>= fun crc ->
  Llio.input_string ic >>= fun cmd ->
  Sn.sn_to buf i;
  Llio.int32_to buf crc;
  Llio.string_to buf cmd;
  Lwt.return ()

let entry_to buf i value =
  Sn.sn_to buf i;
  let b = Buffer.create 64 in
  let () = Value.value_to b value in
  let cmd = Buffer.contents b in
  let crc = Crc32c.calculate_crc32c cmd 0 (String.length cmd) in
  Llio.int32_to buf crc;
  Llio.string_to buf cmd

let write_entry oc i value =
  Sn.output_sn oc i >>= fun () ->
  let b = Buffer.create 64 in
  let () = Value.value_to b value in
  let cmd = Buffer.contents b in
  let chksum = Crc32c.calculate_crc32c cmd 0 (String.length cmd) in
  Llio.output_int32 oc chksum >>= fun() ->
  Llio.output_string oc cmd

let write_marker oc i value m =
  Sn.output_sn oc i >>= fun () ->
  let b = Buffer.create 64 in
  let () = Value.value_to b value in
  let () = Llio.option_to Llio.string_to b m in
  let cmd = Buffer.contents b in
  let crc = Crc32c.calculate_crc32c cmd 0 (String.length cmd) in
  Llio.output_int32 oc crc >>= fun () ->
  Llio.output_string oc cmd


type tlogValidity =
  | TlogValidIncomplete
  | TlogValidComplete
  | TlogInvalid
