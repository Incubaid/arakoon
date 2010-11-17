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

open Update
open Lwt

(* Transaction logs are binary files                                 *)
(* Each log entry is an encoded key-value store modification         *)
(* The structure of an entry is as follows                           *)
(* ---------------------------------------                           *)
(* | I     | Size  | Checksum | Operation |                          *)
(* | Int32 | Int32 | Int32    | VarSize   |                          *)
(* ----------------------------------------                          *)
(* The size is the size in bytes of the stored binary operation      *)
(* The checksum is the checksum of the stored binary operation       *)
(* The operation is the exact same protocol message as what is received
   from the client   *)

exception TLogCheckSumError

let tlogEntriesPerFile = ref (100 * 1000)
let tlogExtension = ".tlog"
let tlogFileRegex = 
  let qex = Str.quote tlogExtension in
  Str.regexp ("[0-9]+" ^ qex ^ "$" )


let lwt_directory_list dn =
  let h = Lwt_unix.opendir dn in
  let rec loop acc  =
    Lwt.catch
      (fun () ->
        Lwt_unix.readdir h >>= fun x ->
        match x with
          | "." | ".." -> loop acc
          | s' -> loop (s' :: acc)
      )
      (function
        | End_of_file -> Lwt.return (List.rev acc)
        | exn -> Lwt.fail exn
      )
  in
  Lwt.finalize
    (fun () -> loop [])
    (fun () -> let () = Lwt_unix.closedir h in Lwt.return ())

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
  Sn.input_sn    ic >>= fun  i     ->
  Llio.input_int32 ic >>= fun chkSum ->
  Llio.input_string ic >>= fun cmd   ->
  (* if you want to do validation, do it here *)
  let chksum2 = Crc32c.calculate_crc32c cmd 0 (String.length cmd) in
  begin
    if chkSum <> chksum2 then
      Lwt.fail TLogCheckSumError
    else Lwt.return ()
  end >>= fun () ->
  let update,_ = Update.from_buffer cmd 0 in
  Lwt.return (i, update)

let entry_from buff pos = 
  let i, pos2  = Sn.sn_from       buff pos  in
  let crc,pos3 = Llio.int32_from  buff pos2 in
  let cmd,pos4 = Llio.string_from buff pos3 in
  let update,_ = Update.from_buffer cmd 0 in
  (i,update), pos4 

let read_into ic buf =
  Sn.input_sn ic >>= fun i ->
  Llio.input_int32 ic >>= fun crc ->
  Llio.input_string ic >>= fun cmd ->
  Sn.sn_to buf i;
  Llio.int32_to buf crc;
  Llio.string_to buf cmd;
  Lwt.return () 
  
let write_entry oc i update =
  Sn.output_sn oc i >>= fun () ->
  let b = Buffer.create 64 in
  let () = Update.to_buffer b update in
  let cmd = Buffer.contents b in
  let chksum = Crc32c.calculate_crc32c cmd 0 (String.length cmd) in
  Llio.output_int32 oc chksum >>= fun() ->
  Llio.output_string oc cmd

type tlogValidity =
  | TlogValidIncomplete
  | TlogValidComplete
  | TlogInvalid


