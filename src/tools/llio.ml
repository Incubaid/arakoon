(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

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

type lwtoc = Lwt_io.output_channel
type lwtic = Lwt_io.input_channel

type namedValue =
  | NAMED_INT of string * int
  | NAMED_INT64 of string * int64
  | NAMED_FLOAT of string * float
  | NAMED_STRING of string * string
  | NAMED_VALUELIST of string * (namedValue list)

let named_value_info = function
  | NAMED_INT       (n, i) -> n, 1
  | NAMED_INT64     (n, i) -> n, 2
  | NAMED_FLOAT     (n, f) -> n, 3
  | NAMED_STRING    (n, s) -> n, 4
  | NAMED_VALUELIST (n, l) -> n, 5

let lwt_failfmt fmt =
  let k x = Lwt.fail (Failure x) in
  Printf.ksprintf k fmt

let (<: ) = Int32.shift_left
let (<::) = Int64.shift_left
let (>: ) = Int32.shift_right_logical
let (>::) = Int64.shift_right_logical
let (|: ) = Int32.logor
let (|::) = Int64.logor

let int32_from buff pos =
  let i32 i= Int32.of_int (Char.code buff.[pos + i]) in
  let b0 = i32 0
  and b1s = i32 1 <: 8
  and b2s = i32 2 <: 16
  and b3s = i32 3 <: 24 in
  let result = b0 |: b1s |: b2s |: b3s
  in result, pos + 4

let int_from buff pos =
  let r,pos' = int32_from buff pos in
  Int32.to_int r ,pos'

let int32_to buffer i32 =
  let char_at n =
    let pos = n * 8 in
    let mask = Int32.of_int 0xff <: pos in
    let code = (Int32.logand i32 mask) >: pos in
    Char.chr (Int32.to_int code)
  in
  let add i = Buffer.add_char buffer (char_at i) in
  add 0;
  add 1;
  add 2;
  add 3

let int_to buffer i = int32_to buffer (Int32.of_int i)

let int64_from buf pos =
  let i64 i= Int64.of_int (Char.code buf.[pos + i]) in
  let b0 = i64 0
  and b1s = i64 1 <:: 8
  and b2s = i64 2 <:: 16
  and b3s = i64 3 <:: 24
  and b4s = i64 4 <:: 32
  and b5s = i64 5 <:: 40
  and b6s = i64 6 <:: 48
  and b7s = i64 7 <:: 56 in
  let r =
    b0 |:: b1s |:: b2s |:: b3s
    |:: b4s |:: b5s |:: b6s |:: b7s
  in
  r,pos + 8

let int64_to buf i64 =
  let char_at n =
    let pos = n * 8 in
    let mask = Int64.of_int 0xff <:: pos in
    let code = (Int64.logand i64 mask) >:: pos in
    Char.chr (Int64.to_int code)
  in
  let set x = Buffer.add_char buf (char_at x) in
  set 0; set 1; set 2; set 3;
  set 4; set 5; set 6; set 7;
  ()


let output_int64 oc i64 =
  let buf = Buffer.create 8 in
  let _ = int64_to buf i64 in
  Lwt_io.write oc (Buffer.contents buf)

let input_int64 ic =
  let buf = String.create 8 in
  Lwt_io.read_into_exactly ic buf 0 8 >>= fun () ->
  let r,_ = int64_from buf 0 in
  Lwt.return r


let float_to buf f =
  let bf = Int64.bits_of_float f in
  int64_to buf bf

let float_from buffer pos =
  let bf,pos' = int64_from buffer pos in
  let f = Int64.float_of_bits bf in
  f,pos'



let string_from buffer pos =
  let size,pos' = int_from buffer pos in
  String.sub buffer (pos+4) size, pos' + size

let string_to buffer s =
  let size = String.length s in
  int_to buffer size;
  Buffer.add_string buffer s

let char_to buffer c = Buffer.add_char buffer c

let char_from buffer pos =
  let c = buffer.[pos] in
  c, pos + 1


let bool_to buffer b =
  let c = if b then '\x01' else '\x00' in
  Buffer.add_char buffer c

let bool_from buffer pos =
  let c = buffer.[pos] in
  let r = match c with
    | '\x00' -> false
    | '\x01' -> true
    | _ -> failwith "not a boolean"
  in r,pos + 1


let output_int32 oc (i:int32) =
  let buf = Buffer.create 4 in
  let () = int32_to buf i in
  let cts = Buffer.contents buf in
  Lwt_io.write oc cts


let output_bool oc (b:bool) =
  let c = if b then '\x01' else '\x00' in
  Lwt_io.write_char oc c

let input_bool ic =
  Lwt_io.read_char ic >>= function
  | '\x00' -> Lwt.return false
  | '\x01' -> Lwt.return true
  | x -> lwt_failfmt "can't be a bool '%c'" x

let input_int32 ic =
  let buf = String.create 4 in
  Lwt_io.read_into_exactly ic buf 0 4 >>= fun () ->
  let r,_ = int32_from buf 0 in
  Lwt.return r


let output_int oc i =
  output_int32 oc (Int32.of_int i)

let input_int ic =
  input_int32 ic >>= fun i32 ->
  Lwt.return (Int32.to_int i32)

let output_string oc (s:string) =
  let size = String.length s in
  output_int32 oc (Int32.of_int size) >>= fun () ->
  Lwt_io.write oc s



let input_string ic =
  input_int ic >>= fun size ->
  if size > (4 * 1024 * 1024 * 1024) then
    lwt_failfmt "Unexpectedly large string size requested:%d" size
  else Lwt.return size  >>= fun size2 ->
    let result = String.create size2 in
    Lwt_io.read_into_exactly ic result 0 size2 >>= fun () ->
    Lwt.return result

let output_string_pair oc (s0,s1) =
  output_string oc s0 >>= fun () ->
  output_string oc s1

let input_string_pair ic =
  input_string ic >>= fun s0 ->
  input_string ic >>= fun s1 ->
  Lwt.return (s0,s1)

let input_listl input_element ic =
  input_int ic >>= fun size ->
  let rec loop acc = function
    | 0 -> Lwt.return (size, acc)
    | i -> input_element ic >>= fun a -> loop (a :: acc) (i -1)
  in
  loop [] size

let input_list input_element ic =
  input_listl input_element ic >>= fun (size,list) ->
  Client_log.debug_f "Received a list of %d elemements" size >>= fun () ->
  Lwt.return list

let input_string_list ic = input_list input_string ic

let input_kv_list ic = input_list input_string_pair ic

let _output_list output_element oc count list =
  output_int oc count >>= fun () ->
  Client_log.debug_f "Outputting list with %d elements" count >>= fun () ->
  Lwt_list.iter_s (output_element oc) list

let output_counted_list output_element oc (count, list) =
  _output_list output_element oc count list

let output_list output_element oc list =
  let count = List.length list in
  _output_list output_element oc count list

let output_string_list oc list = output_list output_string oc list

let output_kv_list oc = output_list output_string_pair oc

let output_array output_element oc es =
  let n = Array.length es in
  output_int oc n >>= fun () ->
  let rec loop i =
    if i = n
    then Lwt.return ()
    else
      begin
        let ei = es.(i) in
        output_element oc ei >>= fun () ->
        loop (i+1)
      end
  in
  loop 0

let output_array_reversed output_element oc es =
  let n = Array.length es in
  output_int oc n >>= fun () ->
  let rec loop i =
    if i < 0
    then Lwt.return ()
    else
      begin
        let ei = es.(i) in
        output_element oc ei >>= fun () ->
        loop (i-1)
      end
  in
  loop (n-1)

let output_string_array_reversed oc strings =
  output_array_reversed output_string oc strings

let list_to buf e_to list =
  int_to buf (List.length list);
  List.iter (e_to buf) (List.rev list)

let string_list_to buf sl = list_to buf string_to sl

let list_from s e_from pos =
  let size,p0 = int_from s pos in
  let rec loop acc p = function
    | 0 -> acc,p
    | i -> let e,p' = e_from s p in
      loop (e::acc) p' (i-1)
  in loop [] p0 size

let string_list_from s pos = list_from s string_from pos

let output_string_option oc = function
  | None -> output_bool oc false
  | Some s ->
    output_bool oc true >>= fun () ->
    output_string oc s

let option_to (f:Buffer.t -> 'a -> unit) buff =  function
  | None -> bool_to buff false
  | Some (v:'a) ->
    bool_to buff true;
    f buff v

let option_from (f:string -> int -> 'a * int) string pos =
  let b, pos1 = bool_from string pos in
  match b with
    | false -> None, pos1
    | true -> let v, pos2 = f string pos1 in
      (Some v), pos2

let string_option_to buff so =  option_to string_to buff so

let string_option_from string pos = option_from string_from string pos



let input_string_option ic =
  input_bool ic >>= function
  | false -> Lwt.return None
  | true -> (input_string ic >>= fun s -> Lwt.return (Some s))

let hashtbl_to buf e2 h =
  let len = Hashtbl.length h in
  int_to buf len;
  Hashtbl.iter (fun k v -> e2 buf k v) h

let hashtbl_from buf ef pos =
  let len,p1 = int_from buf pos in
  let r = Hashtbl.create len in
  let rec loop pos = function
    | 0 -> r, pos
    | i -> let (k,v), p2 = ef buf pos in
      let () = Hashtbl.add r k v in
      loop p2 (i-1)
  in
  loop p1 len


let copy_stream ~length ~ic ~oc =
  Client_log.debug_f "copy_stream ~length:%Li" length >>= fun () ->
  let bs = Lwt_io.default_buffer_size () in
  let bs64 = Int64.of_int bs in
  let buffer = String.create bs in
  let n_bs = Int64.div length bs64 in
  let rest = Int64.to_int (Int64.rem length bs64) in
  let rec loop i =
    if i = Int64.zero
    then
      begin
        Lwt_io.read_into_exactly ic buffer 0 rest >>= fun () ->
        Lwt_io.write_from_exactly oc buffer 0 rest
      end
    else
      begin
        Lwt_io.read_into_exactly ic buffer 0 bs >>= fun () ->
        Lwt_io.write oc buffer >>= fun () ->
        loop (Int64.pred i)
      end
  in
  loop n_bs >>= fun () ->
  Client_log.debug "done: copy_stream"

let rec named_field_to (buffer: Buffer.t) (field: namedValue) : unit =
  let field_name, field_type = named_value_info field in
  int_to buffer field_type;
  string_to buffer field_name;
  match field with
    | NAMED_INT (_, i) ->
      int_to buffer i
    | NAMED_FLOAT (_, f) ->
      float_to buffer f
    | NAMED_INT64 (_, i) ->
      int64_to buffer i
    | NAMED_STRING (_, s) ->
      string_to buffer s
    | NAMED_VALUELIST (_, l) ->
      int_to buffer (List.length l);
      let encode_entry = named_field_to buffer in
      List.iter encode_entry l

let rec named_field_from buffer offset: (namedValue*int) =
  let field_type, offset = int_from buffer offset in
  let field_name, offset = string_from buffer offset in
  begin
    match field_type with
      | 1 ->
        let i, offset = int_from buffer offset in
        NAMED_INT(field_name, i), offset
      | 2 ->
        let i64, offset = int64_from buffer offset in
        NAMED_INT64(field_name, i64), offset
      | 3 ->
        let f, offset = float_from buffer offset in
        NAMED_FLOAT(field_name, f), offset
      | 4 ->
        let s, offset = string_from buffer offset in
        NAMED_STRING(field_name, s), offset
      | 5 ->
        begin
          let rec decode_loop decoded offset= function
            | 0 -> (List.rev decoded), offset
            | i ->
              let new_elem, offset = named_field_from buffer offset in
              decode_loop (new_elem :: decoded) offset (i-1)
          in
          let length, offset = int_from buffer offset in
          let decoded, offset = decode_loop [] offset length in
          NAMED_VALUELIST(field_name, decoded), offset
        end
      | _ -> failwith "Unknown value type. Cannot decode."
  end

let input_hashtbl fk fv ic =
  input_int ic >>= fun elem_cnt ->
  let result = Hashtbl.create elem_cnt in
  let rec helper = function
    | 0 -> Lwt.return result
    | i ->
      begin
        fk ic >>= fun key ->
        fv ic >>= fun value ->
        Hashtbl.replace result key value;
        helper (i-1)
      end
  in
  helper elem_cnt

let output_hashtbl out_f oc ht =
  let l =  Hashtbl.length ht in
  output_int oc l >>= fun () ->
  let helper a b x =
    x >>= fun () ->
    out_f oc a b
  in
  Hashtbl.fold helper ht (Lwt.return ())
