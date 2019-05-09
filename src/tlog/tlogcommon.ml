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
exception TLogSabotage

let tlogExtension = ".tlog"
let tlogFileRegex =
  let qex = Str.quote tlogExtension in
  Str.regexp ("[0-9]+" ^ qex ^ "$" )



let isValidSuccessor i prevI =
  i = prevI || i = Sn.succ prevI

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
       let buf = Llio.make_buffer cmd 0 in
       let value = Value.value_from buf in
       let marker =
         if Llio.buffer_pos buf = cmdl
         then None
         else
           let m = Llio.string_option_from buf in
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


let entry_from buff =
  let i  = Sn.sn_from       buff in
  let _  = Llio.int32_from  buff in
  let cmd = Llio.string_from buff in
  let value = Value.value_from (Llio.make_buffer cmd 0) in
  let e = Entry.make i value 0L None in
  e


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
  Llio.output_string oc cmd >>= fun () ->
  let extra_size = 16 (* i:8 + chksum:4 + string:4 *) in
  let total_size = Bytes.length cmd + extra_size in
  Lwt.return total_size

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
