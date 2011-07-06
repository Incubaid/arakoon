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

let lwt_failfmt fmt = 
  let k x = Lwt.fail (Failure x) in
  Printf.ksprintf k fmt


let int32_from buff pos =
  let i32 i= Int32.of_int (Char.code buff.[pos + i]) in
  let b0 = i32 0
  and b1 = i32 1
  and b2 = i32 2
  and b3 = i32 3 in
  let (<<) = Int32.shift_left
  and (||) = Int32.logor in
  let result =
    b0 || (b1 << 8) || (b2 << 16) || (b3 << 24)
  in result, pos + 4

let int_from buff pos =
  let r,pos' = int32_from buff pos in
    Int32.to_int r ,pos'

let int32_to buffer i32 =
  let (<<) = Int32.shift_left in
  let (>>) = Int32.shift_right_logical in
  let char_at n =
    let pos = n * 8 in
    let mask = Int32.of_int 0xff << pos in
    let code = (Int32.logand i32 mask)  >> pos in
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
    let b0 = i64 0 in
    let b1 = i64 1 in
    let b2 = i64 2 in
    let b3 = i64 3 in
    let b4 = i64 4 in
    let b5 = i64 5 in
    let b6 = i64 6 in
    let b7 = i64 7 in
    let (<<) = Int64.shift_left
    and (||) = Int64.logor in
    let r =
      b0 || (b1 << 8)  || (b2 << 16) || (b3 << 24) ||
	(b4 <<32)|| (b5 <<40)  || (b6 << 48) || (b7 << 56)
    in
      r,pos + 8 

let int64_to buf i64 =
  let (<<) = Int64.shift_left
  and (>>>) = Int64.shift_right_logical in
  let char_at n =
    let pos = n * 8 in
    let mask = Int64.of_int 0xff << pos in
    let code = (Int64.logand i64 mask) >>> pos in
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
    if size > (16*1024*1024) then
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

let input_list input_element ic =
  input_int ic >>= fun size ->
  let rec loop i acc = 
    if i = 0
    then Lwt.return acc
    else input_element ic >>= fun s -> loop (i-1) (s:: acc)
  in
  loop size []

let output_list output_element oc list = 
  output_int oc (List.length list)  >>= fun () ->
  Lwt_list.iter_s (output_element oc) list 

let list_to buf e_to list =
  int_to buf (List.length list);
  List.iter (e_to buf) (List.rev list)

let list_from s e_from pos = 
  let size,p0 = int_from s pos in
  let rec loop acc p = function
    | 0 -> acc
    | i -> let e,p' = e_from s p in
	   loop (e::acc) p' (i-1)
  in loop [] p0 size


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
    | i -> let (k,v), p1 = ef buf pos in
	   let () = Hashtbl.add r k v in
	   loop p1 (i-1)
  in
  loop pos len 


let copy_stream ~length ~ic ~oc =
  Lwt_log.debug_f "copy_stream ~length:%Li" length >>= fun () ->
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
  Lwt_log.debug "done: copy_stream"

