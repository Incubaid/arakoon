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
open Routing
open Client_cfg
open Ncfg

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
  | ASSERTEXISTS
  | SET
  | DELETE
  | RANGE
  | PREFIX_KEYS
  | TEST_AND_SET
  | LAST_ENTRIES
  | LAST_ENTRIES2
  | RANGE_ENTRIES
  | MIGRATE_RANGE
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
  | SET_ROUTING_DELTA
  | GET_KEY_COUNT
  | GET_DB
  | CONFIRM
  | GET_FRINGE
  | SET_NURSERY_CFG
  | GET_NURSERY_CFG
  | REV_RANGE_ENTRIES
  | SYNCED_SEQUENCE
  | OPT_DB
  | DEFRAG_DB
  | DELETE_PREFIX
  | VERSION
  | DROP_MASTER
  | MULTI_GET_OPTION
  | CURRENT_STATE


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
  GET_FRINGE              , 0x1dl;
  GET_INTERVAL            , 0x1el;
  SET_NURSERY_CFG         , 0x1fl;
  GET_NURSERY_CFG         , 0x20l;
  SET_ROUTING_DELTA       , 0x21l;
  MIGRATE_RANGE           , 0x22l;
  REV_RANGE_ENTRIES       , 0x23l;
  SYNCED_SEQUENCE         , 0x24l;
  OPT_DB                  , 0x25l;
  DEFRAG_DB               , 0x26l;
  DELETE_PREFIX           , 0x27l;
  VERSION                 , 0x28l;
  ASSERTEXISTS            , 0x29l;
  DROP_MASTER             , 0x30l;
  MULTI_GET_OPTION        , 0x31l;
  CURRENT_STATE           , 0x32l;
  LAST_ENTRIES2           , 0x40l;
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
    | rc32 ->
      Client_log.debug_f "Client operation failed: %ld" rc32 >>= fun () ->
      Llio.input_string ic >>= fun msg ->
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

let assert_exists_to ~allow_dirty buffer key=
  command_to buffer ASSERTEXISTS;
  Llio.bool_to buffer allow_dirty;
  Llio.string_to buffer key

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

let rev_range_entries_to b ~allow_dirty first finc last linc max =
  command_to b REV_RANGE_ENTRIES;
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
  Llio.string_list_to b keys

let multiget_option_to b ~allow_dirty keys =
  command_to b MULTI_GET_OPTION;
  Llio.bool_to b allow_dirty;
  Llio.string_list_to b keys

let who_master_to b =
  command_to b WHO_MASTER

let expect_progress_possible_to b =
  command_to b EXPECT_PROGRESS_POSSIBLE

let ping_to b client_id cluster_id =
  command_to b PING;
  Llio.string_to b client_id;
  Llio.string_to b cluster_id

let version_to b = command_to b VERSION
let get_key_count_to b =
  command_to b GET_KEY_COUNT

let get_nursery_cfg_to b =
  command_to b GET_NURSERY_CFG

let set_nursery_cfg_to b cluster_id client_cfg =
  command_to b SET_NURSERY_CFG;
  Llio.string_to b cluster_id;
  ClientCfg.cfg_to b client_cfg

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

let get_fringe (ic,oc) boundary direction =
  let outgoing buf =
    command_to buf GET_FRINGE;
    Llio.string_option_to buf boundary;
    match direction with
      | Routing.UPPER_BOUND -> Llio.int_to buf 0
      | Routing.LOWER_BOUND -> Llio.int_to buf 1
  in
  request  oc outgoing >>= fun () ->
  Client_log.debug "get_fringe request sent" >>= fun () ->
  response ic Llio.input_kv_list


let set_interval(ic,oc) iv =
  Client_log.debug "set_interval" >>= fun () ->
  let outgoing buf =
    command_to buf SET_INTERVAL;
    Interval.interval_to buf iv
  in
  request  oc outgoing >>= fun () ->
  Client_log.debug "set_interval request sent" >>= fun () ->
  response ic nothing

let get_interval (ic,oc) =
  Client_log.debug "get_interval" >>= fun () ->
  let outgoing buf =
    command_to buf GET_INTERVAL
  in
  request oc outgoing >>= fun () ->
  Client_log.debug "get_interval request sent" >>= fun () ->
  response ic Interval.input_interval

let get_routing (ic,oc) =
  let outgoing buf = command_to buf GET_ROUTING
  in
    request  oc outgoing >>= fun () ->
    response ic Routing.input_routing


let set_routing (ic,oc) r =
  let outgoing buf =
    command_to buf SET_ROUTING;
    let b' = Buffer.create 100 in
    Routing.routing_to b' r;
      let size = Buffer.length b' in
      Llio.int_to buf size;
      Buffer.add_buffer buf b'
    in
    request  oc outgoing >>= fun  () ->
    response ic nothing

let set_routing_delta (ic,oc) left sep right =
  let outgoing buf =
    command_to buf SET_ROUTING_DELTA;
    Llio.string_to buf left;
    Llio.string_to buf sep;
    Llio.string_to buf right;
  in
  Client_log.debug "Changing routing" >>= fun () ->
  request oc outgoing >>= fun () ->
  Client_log.debug "set_routing_delta sent" >>= fun () ->
  response ic nothing

let delete_prefix (ic,oc) prefix =
  let outgoing buf =
    command_to buf DELETE_PREFIX;
    Llio.string_to buf prefix
  in
  Client_log.debug_f "delete_prefix %S" prefix >>= fun () ->
  request oc outgoing >>= fun () ->
  response ic Llio.input_int


let _build_sequence_request buf changes =
  let update_buf = Buffer.create (32 * List.length changes) in
  let rec c2u = function
    | Arakoon_client.Set (k,v) -> Update.Set(k,v)
    | Arakoon_client.Delete k -> Update.Delete k
    | Arakoon_client.TestAndSet (k,vo,v) -> Update.TestAndSet (k,vo,v)
    | Arakoon_client.Sequence cs -> Update.Sequence (List.map c2u cs)
    | Arakoon_client.Assert(k,vo) -> Update.Assert(k,vo)
    | Arakoon_client.Assert_exists(k) -> Update.Assert_exists(k)
  in
  let updates = List.map c2u changes in
  let seq = Update.Sequence updates in
  let () = Update.to_buffer update_buf seq in
  let () = Llio.string_to buf (Buffer.contents update_buf)
  in ()

let migrate_range (ic,oc) interval changes =
  let outgoing buf =
    command_to buf MIGRATE_RANGE;
    Interval.interval_to buf interval;
    _build_sequence_request buf changes
  in
  request  oc (fun buf -> outgoing buf) >>= fun () ->
  response ic nothing


let _sequence (ic,oc) changes cmd =
  let outgoing buf =
    command_to buf cmd;
    _build_sequence_request buf changes
  in
  request  oc (fun buf -> outgoing buf) >>= fun () ->
  response ic nothing

let sequence conn changes = _sequence conn changes SEQUENCE

let synced_sequence conn changes = _sequence conn changes SYNCED_SEQUENCE


let get_nursery_cfg (ic,oc) =
  let decode ic =
    Llio.input_string ic >>= fun buf ->
    let cfg, pos = NCFG.ncfg_from buf 0 in
    Lwt.return cfg
  in
  request oc get_nursery_cfg_to >>= fun () ->
  response ic decode

let set_nursery_cfg (ic,oc) clusterid cfg =
  let outgoing buf =
     set_nursery_cfg_to buf clusterid cfg
  in
  request oc outgoing >>= fun () ->
  response ic nothing

let optimize_db (ic,oc) =
   let outgoing buf =
      command_to buf OPT_DB
   in
   request oc outgoing >>= fun () ->
   response ic nothing

let defrag_db (ic,oc) =
  let outgoing buf = command_to buf DEFRAG_DB in
  request oc outgoing >>= fun () ->
  response ic nothing

let version (ic,oc) =
  let outgoing buf = command_to buf VERSION in
  request oc outgoing >>= fun () ->
  response ic
    (fun ic ->
      Llio.input_int ic >>= fun major ->
      Llio.input_int ic >>= fun minor ->
      Llio.input_int ic >>= fun patch ->
      Llio.input_string ic >>= fun info ->
      Lwt.return (major,minor,patch,info)
    )

let current_state (ic,oc) =
  let outgoing buf = command_to buf CURRENT_STATE in
  request oc outgoing >>= fun () ->
  response ic (fun ic -> Llio.input_string ic)

let drop_master (ic, oc) =
  let outgoing buf = command_to buf DROP_MASTER in
  request oc outgoing >>= fun () ->
  response ic nothing

exception XException of Arakoon_exc.rc * string
