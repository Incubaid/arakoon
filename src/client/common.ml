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

let _MAGIC = 0xb1ff0000l
let _MASK  = 0x0000ffffl
let _VERSION = 0
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


let code2int = [
  PING,                    0x1l ;
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
]

let int2code = List.fold_left (fun acc (a,b) -> (b,a)::acc) [] code2int

let command_to buffer command =
  let c = List.assoc command code2int in
  let masked = Int32.logor c _MAGIC in
  Llio.int32_to buffer masked

let send_command oc command =
  let c = List.assoc command code2int in
  let masked = Int32.logor c _MAGIC in
  Llio.output_int32 oc masked

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
	  Llio.input_string ic >>= fun key ->
          Llio.input_string ic >>= fun value ->
	  result.(i) <- (key , value);
	  loop (i+1)
	end
    in loop 0

let value_list ic =
  Llio.input_int ic >>= fun size ->
  let rec loop i acc =
    if i = size
    then Lwt.return acc
    else Llio.input_string ic >>= fun s -> loop (i+1) (s :: acc)
  in loop 0 []

let key_list = value_list

let kv_list ic =
  Llio.input_int ic >>= fun size ->
  let rec loop i acc =
    if i = size
    then Lwt.return acc
    else
      Llio.input_string ic >>= fun k ->
    Llio.input_string ic >>= fun v ->
    loop (i+1) ((k,v) :: acc)
  in loop 0 []

let response ic f =
  Llio.input_int32 ic >>= function
    | 0l -> f ic
    | rc32 -> Llio.input_string ic >>= fun msg ->
      let rc = Arakoon_exc.rc_of_int32 rc32 in
      Lwt.fail (Arakoon_exc.Exception (rc, msg))

let exists_to buffer key =
  command_to buffer EXISTS;
  Llio.string_to buffer key

let get_to buffer key =
  command_to buffer GET;
  Llio.string_to buffer key

let set_to buffer key value =
  command_to buffer SET;
  Llio.string_to buffer key;
  Llio.string_to buffer value

let delete_to buffer key =
  command_to buffer DELETE;
  Llio.string_to buffer key

let range_to b first finc last linc max =
  command_to b RANGE;
  Llio.string_option_to b first;
  Llio.bool_to b finc;
  Llio.string_option_to b last;
  Llio.bool_to b linc;
  Llio.int_to b max

let range_entries_to b first finc last linc max =
  command_to b RANGE_ENTRIES;
  Llio.string_option_to b first;
  Llio.bool_to b finc;
  Llio.string_option_to b last;
  Llio.bool_to b linc;
  Llio.int_to b max

let prefix_keys_to b prefix max =
  command_to b PREFIX_KEYS;
  Llio.string_to b prefix;
  Llio.int_to b max

let test_and_set_to b key expected wanted =
  command_to b TEST_AND_SET;
  Llio.string_to b key;
  Llio.string_option_to b expected;
  Llio.string_option_to b wanted

let multiget_to b keys =
  command_to b MULTI_GET;
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

(*
let sequence b updates =
  command_to b SEQUENCE;
  Llio.int_to b (List.length updates);
  let do_one = function
    | _ -> failwith "not implemented" 
  in
  List.iter do_one updates
*)

exception XException of Arakoon_exc.rc * string
