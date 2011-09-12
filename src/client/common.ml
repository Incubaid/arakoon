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
open Interval
open Update

let _MAGIC = 0xb1ff0000l
let _MASK  = 0x0000ffffl
let _VERSION = 1
(*
let _STRLEN_SIZE = 4
let _CMD_SIZE   = 4
let _BOOL_SIZE   = 1
let _INT_SIZE  = 4 
*)


type client_command =
  | PING
  | WHO_MASTER
  | EXISTS
  | GET
  | ASSERT
  | SET
  | DELETE
  | RANGE
  | PREFIX_KEYS
  | TEST_AND_SET
  | LAST_ENTRIES
  | RANGE_ENTRIES
  | SEQUENCE
  | MULTI_GET
  | EXPECT_PROGRESS_POSSIBLE
  | STATISTICS
  | COLLAPSE_TLOGS
  | USER_FUNCTION
  | GET_INTERVAL
  | SET_INTERVAL
  | GET_ROUTING
  | SET_ROUTING
  | GET_KEY_COUNT
  | GET_DB
  | CONFIRM
  | GET_TAIL

let code2int = [
  PING,                     0x1l ;
  WHO_MASTER,               0x2l ;
  EXISTS,                   0x7l ;
  GET,                      0x8l ;
  SET,                      0x9l ;
  DELETE,                   0xal ;
  RANGE,                    0xbl ;
  PREFIX_KEYS,              0xcl ;
  TEST_AND_SET,             0xdl ;
  LAST_ENTRIES,             0xel ;
  RANGE_ENTRIES,            0xfl ;
  SEQUENCE,                 0x10l;
  MULTI_GET,                0x11l;
  EXPECT_PROGRESS_POSSIBLE, 0x12l;
  STATISTICS              , 0x13l;
  COLLAPSE_TLOGS          , 0x14l;
  USER_FUNCTION           , 0x15l;
  ASSERT                  , 0x16l;
  SET_INTERVAL            , 0x17l;
  GET_ROUTING             , 0x18l;
  SET_ROUTING             , 0x19l;
  GET_KEY_COUNT           , 0x1al;
  GET_DB                  , 0x1bl;
  CONFIRM                 , 0x1cl;
  GET_TAIL                , 0x1dl;
  GET_INTERVAL            , 0x1el;
]

let int2code = 
  let r = Hashtbl.create 41 in
  let () = List.iter (fun (a,b) -> Hashtbl.add r b a) code2int in
  r

let lookup_code i32 = Hashtbl.find int2code i32

let command_to buffer command =
  let c = List.assoc command code2int in
  let masked = Int32.logor c _MAGIC in
  Llio.int32_to buffer masked

let nothing = fun ic -> Lwt.return ()

let value_array ic =
  Llio.input_int ic >>= fun size ->
  let result = Array.create size "" in
  let rec loop i =
    if i = size
    then Lwt.return result
    else
      begin
	Llio.input_string ic >>= fun s ->
	result.(i) <- s;
	loop (i+1)
      end
  in loop 0

let kv_array ic =
  Llio.input_int ic >>= fun size ->
    let result = Array.create size ("","") in
    let rec loop i =
      if i = size
      then Lwt.return result
      else
	begin
	  Llio.input_string_pair ic >>= fun p ->
	  result.(i) <- p;
	  loop (i+1)
	end
    in loop 0


let request oc f =
  let buf = Buffer.create 32 in
  let () = f buf in
  Lwt_io.write oc (Buffer.contents buf) >>= fun () ->
  Lwt_io.flush oc

let response ic f =
  Llio.input_int32 ic >>= function
    | 0l -> f ic
    | rc32 -> Llio.input_string ic >>= fun msg ->
      let rc = Arakoon_exc.rc_of_int32 rc32 in
      Lwt.fail (Arakoon_exc.Exception (rc, msg))

let exists_to buffer ~allow_dirty key =
  command_to buffer EXISTS;
  Llio.bool_to buffer allow_dirty;
  Llio.string_to buffer key

let get_to ~allow_dirty buffer key =
  command_to buffer GET;
  Llio.bool_to buffer allow_dirty;
  Llio.string_to buffer key

let assert_to ~allow_dirty buffer key vo = 
  command_to buffer ASSERT;
  Llio.bool_to buffer allow_dirty;
  Llio.string_to buffer key;
  Llio.string_option_to buffer vo

let set_to buffer key value =
  command_to buffer SET;
  Llio.string_to buffer key;
  Llio.string_to buffer value

let confirm_to buffer key value = 
  command_to buffer CONFIRM;
  Llio.string_to buffer key;
  Llio.string_to buffer value

let delete_to buffer key =
  command_to buffer DELETE;
  Llio.string_to buffer key

let range_to b ~allow_dirty first finc last linc max =
  command_to b RANGE;
  Llio.bool_to b allow_dirty;
  Llio.string_option_to b first;
  Llio.bool_to b finc;
  Llio.string_option_to b last;
  Llio.bool_to b linc;
  Llio.int_to b max

let range_entries_to b ~allow_dirty first finc last linc max =
  command_to b RANGE_ENTRIES;
  Llio.bool_to b allow_dirty;
  Llio.string_option_to b first;
  Llio.bool_to b finc;
  Llio.string_option_to b last;
  Llio.bool_to b linc;
  Llio.int_to b max

let prefix_keys_to b ~allow_dirty prefix max =
  command_to b PREFIX_KEYS;
  Llio.bool_to b allow_dirty;
  Llio.string_to b prefix;
  Llio.int_to b max

let test_and_set_to b key expected wanted =
  command_to b TEST_AND_SET;
  Llio.string_to b key;
  Llio.string_option_to b expected;
  Llio.string_option_to b wanted

let user_function_to b name po = 
  command_to b USER_FUNCTION;
  Llio.string_to b name;
  Llio.string_option_to b po

let multiget_to b ~allow_dirty keys =
  command_to b MULTI_GET;
  Llio.bool_to b allow_dirty;
  Llio.int_to b (List.length keys);
  List.iter (Llio.string_to b) keys

let who_master_to b =
  command_to b WHO_MASTER

let expect_progress_possible_to b =
  command_to b EXPECT_PROGRESS_POSSIBLE

let ping_to b client_id cluster_id =
  command_to b PING;
  Llio.string_to b client_id;
  Llio.string_to b cluster_id

let get_key_count_to b =
  command_to b GET_KEY_COUNT
  

let prologue cluster (_,oc) =
  Llio.output_int32  oc _MAGIC >>= fun () ->
  Llio.output_int    oc _VERSION >>= fun () ->
  Llio.output_string oc cluster 


let who_master (ic,oc) = 
  request  oc (fun buf -> who_master_to buf) >>= fun () ->
  response ic Llio.input_string_option

let set (ic,oc) key value = 
  request  oc (fun buf -> set_to buf key value) >>= fun () ->
  response ic nothing

let get (ic,oc) ~allow_dirty key = 
  request  oc (fun buf -> get_to ~allow_dirty buf key) >>= fun () ->
  response ic Llio.input_string

let get_tail (ic,oc) lower = 
  let outgoing buf = 
    command_to buf GET_TAIL;
    Llio.string_to buf lower
  in
  request  oc outgoing >>= fun () ->
  response ic Llio.input_kv_list


let set_interval(ic,oc) iv = 
  Lwt_log.debug "set_interval" >>= fun () ->
  let outgoing buf = 
    command_to buf SET_INTERVAL;
    Interval.interval_to buf iv
  in
  request  oc outgoing >>= fun () ->
  response ic nothing

let get_interval (ic,oc) = 
  Lwt_log.debug "get_interval" >>= fun () ->
  let outgoing buf = 
    command_to buf GET_INTERVAL
  in
  request oc outgoing >>= fun () ->
  response ic Interval.input_interval

let sequence (ic,oc) changes = 
  let outgoing buf = 
    command_to buf SEQUENCE;
    let update_buf = Buffer.create (32 * List.length changes) in
    let rec c2u = function
      | Arakoon_client.Set (k,v) -> Update.Set(k,v)
      | Arakoon_client.Delete k -> Update.Delete k
      | Arakoon_client.TestAndSet (k,vo,v) -> Update.TestAndSet (k,vo,v)
      | Arakoon_client.Sequence cs -> Update.Sequence (List.map c2u cs)
      | Arakoon_client.Assert(k,vo) -> Update.Assert(k,vo)
    in
    let updates = List.map c2u changes in
    let seq = Update.Sequence updates in
    let () = Update.to_buffer update_buf seq in
    let () = Llio.string_to buf (Buffer.contents update_buf)
    in () 
  in
  request  oc (fun buf -> outgoing buf) >>= fun () ->
  response ic nothing
    

exception XException of Arakoon_exc.rc * string
