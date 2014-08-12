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



open Update

type content =
  | Vc of (Update.t list * bool) (* is_synced *)
  | Vm of (string * float)

type t = Checksum.Crc32.t option * content

exception ValueCheckSumError of Sn.t * t

let content_to buf = function
  | Vc (us,synced) -> begin
      Llio.char_to buf 'c';
      Llio.bool_to buf synced;
      Llio.list_to buf Update.to_buffer us
    end
  | Vm (m,l) -> begin
      Llio.char_to buf 'm';
      Llio.string_to buf m;
      Llio.int64_to buf (Int64.of_float l)
    end

let value_to buf (cso, c) =
  match cso with
    | None -> begin
        Llio.int_to buf 0xff;
        content_to buf c
      end
    | Some cs -> begin
        Llio.int_to buf 0x100;
        Checksum.Crc32.checksum_to buf cs;
        content_to buf c
      end

let content_from buf =
  let c = Llio.char_from buf in
  match c with
  | 'c' ->
    let synced = Llio.bool_from buf  in
    let us     = Llio.list_from buf Update.from_buffer in
    Vc (us, synced)
  | 'm' ->
    let m = Llio.string_from buf in
    let l = Llio.int64_from buf in
    Vm (m, Int64.to_float l)
  | _ -> failwith "demarshalling error"

let value_from buf =
  let pos = Llio.buffer_pos buf in
  match Llio.int_from buf with
  | 0x100 ->
    let cs = Checksum.Crc32.checksum_from buf in
    (Some cs, content_from buf)
  | 0xff -> (None, content_from buf)
  | _ ->
    (* this is for backward compatibility:
       formerly, we logged updates iso values *)
    let () = Llio.buffer_set_pos buf pos in
    let u = Update.from_buffer buf in
    let synced = Update.is_synced u in
    (None, Vc ([u], synced))

let value2s ?(values=false) (cso, c) =
  let css = match cso with
    | None -> "_"
    | Some cs -> Checksum.Crc32.string_of cs
  in
  match c with
  | Vc (us,synced) ->
    let uss = Log_extra.list2s (fun u -> Update.update2s u ~values) us in
    Printf.sprintf "(%s, Vc (%s,%b)" css uss synced
  | Vm (m,l) -> Printf.sprintf "(%s, Vm (%s,%f))" css m l

let _string_of_content c =
  let buf = Buffer.create 64 in
  let () = content_to buf c in
  Buffer.contents buf

let checksum tlog_coll i c =
  let s = _string_of_content c in
  match tlog_coll # get_last_value (Sn.pred i) with
  | None -> Checksum.Crc32.calculate s
  | Some (cso, _) -> Checksum.Crc32.update cso s

let create_client_value_nocheck us synced = (None, Vc (us, synced))

let create_master_value_nocheck m l = (None, Vm (m, l))

let create_first_value c =
  let s = _string_of_content c in
  let cs = Checksum.Crc32.calculate s in
  (Some cs, c)

let create_value tlog_coll i c =
  let cs = checksum tlog_coll i c in
  (Some cs, c)

let create_first_client_value us synced =
  let c = Vc (us, synced) in
  create_first_value c

let create_client_value tlog_coll i us synced =
  let c = Vc (us, synced) in
  create_value tlog_coll i c

let create_first_master_value m l =
  let c = Vm (m, l) in
  create_first_value c

let create_master_value tlog_coll i m l =
  let c = Vm (m, l) in
  create_value tlog_coll i c

let validate tlog_coll i (cso, c) =
  match cso with
    | None -> true
    | Some cs -> cs = checksum tlog_coll i c

let is_master_set = function
  | (_, Vc _) -> false
  | (_, Vm _) -> true

let is_other_master_set me = function
  | (_, Vm (m, _)) -> m <> me
  | (_, Vc _) -> false

let is_synced = function
  | (_, Vc (_,s)) -> s
  | (_, Vm _) -> false

let clear_self_master_set me v = match v with
  | (_, Vm (m, _)) -> if m = me then create_master_value_nocheck m 0.0 else v
  | (_, Vc _) -> v

let fill_if_master_set = function
  | (_, Vm (m, _)) ->
    let now = Unix.gettimeofday () in
    create_master_value_nocheck m now
  | (_, Vc _) as v -> v

let updates_from_value = function
  | (_, Vc (us,_)) -> us
  | (_, Vm (m,l)) -> [Update.MasterSet(m,l)]

