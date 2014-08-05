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

open Tlogcommon
open Lwt

let dump_tlog filename ~values=
  let printer () entry =
    let i = Entry.i_of entry
    and v = Entry.v_of entry
    and m = Entry.m_of entry in
    let ms = match m with None -> "" | Some s -> Printf.sprintf ":%S" s
    in
    Lwt_io.printlf "%s:%s%s" (Sn.string_of i)
      (Value.value2s v ~values) ms
  in
  let folder,_,index = Tlc2.folder_for filename None in

  let t =
    begin
      let do_it ic =

        let lowerI = Sn.start
        and higherI = None
        and first = Sn.of_int 0
        and a0 = () in
        folder ic ~index lowerI higherI ~first a0 printer >>= fun () ->
        Lwt.return 0
      in
      Lwt_io.with_file ~mode:Lwt_io.input filename do_it
    end
  in
  Lwt_main.run t

let _last_entry filename =
  let last = ref None in
  let printer () entry =
    let i = Entry.i_of entry in
    let p = Entry.p_of entry in
    let () = last := Some entry in
    Lwt_io.printlf "%s:%Li" (Sn.string_of i) p
  in
  let folder,_,index = Tlc2.folder_for filename None in
  let do_it ic =
    let lowerI = Sn.start in
    let higherI = None
    and first = Sn.of_int 0
    and a0 = () in
    folder ic ~index lowerI higherI ~first a0 printer
  in
  Lwt_io.with_file ~mode:Lwt_io.input filename do_it >>= fun () ->
  Lwt.return !last


let strip_tlog filename =
  let maybe_truncate =
    function
      | None -> Lwt.return 0
      | Some e ->
        if Entry.has_marker e
        then
          begin
            let p = Entry.p_of e in
            let pi = Int64.to_int p in
            Lwt_io.printlf "last=%s => truncating to %i" (Entry.entry2s e) pi >>= fun () ->
            Lwt_unix.truncate filename pi >>= fun () ->
            Lwt.return 0
          end
        else
          begin
            Lwt_io.eprintlf "no marker found, not truncating" >>= fun () ->
            Lwt.return 1
          end
  in
  let t =
    _last_entry filename >>= fun last ->
    maybe_truncate last
  in
  Lwt_main.run t

let mark_tlog file_name node_name =
  let t =
    _last_entry file_name >>= fun last ->
    match last with
      | None -> Lwt.return 0
      | Some e ->
        let i     = Entry.i_of e
        and value = Entry.v_of e
        in
        let f oc = Tlogcommon.write_marker oc i value (Some node_name) in
        Lwt_io.with_file
          ~mode:Lwt_io.output
          ~flags:[Unix.O_APPEND;Unix.O_WRONLY]
          file_name f
        >>= fun () -> Lwt.return 0
  in
  Lwt_main.run t

let make_tlog tlog_name (i:int) =
  let sni = Sn.of_int i in
  let t =
    let f oc = Tlogcommon.write_entry oc sni
                 (Value.create_first_client_value [Update.Update.Nop] false)
    in
    Lwt_io.with_file ~mode:Lwt_io.output tlog_name f
  in
  Lwt_main.run t;0


let compress_tlog tlu archive_type=
  let compressor =
    match archive_type with
    | ".tls" | "tls" -> Compression.Snappy
    | _ -> Compression.Bz2
  in
  let failwith x =
    Printf.ksprintf failwith x in
  let () = if not (Sys.file_exists tlu) then failwith "Input file %s does not exist" tlu in
  let tlx = Tlc2.to_archive_name compressor tlu in
  let () = if Sys.file_exists tlx then failwith "Can't compress %s as %s already exists" tlu tlx in
  let t =
    Compression.compress_tlog ~cancel:(ref false) tlu tlx compressor >>= fun () ->
    File_system.unlink tlu
  in
  Lwt_main.run t;
  0

let uncompress_tlog tlx =
  let t =
    let tlu = Tlc2.to_tlog_name tlx in
    Compression.uncompress_tlog tlx tlu >>= fun () ->
    Lwt_unix.unlink tlx
  in
  Lwt_main.run t;0
