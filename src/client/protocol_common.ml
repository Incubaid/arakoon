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
open Arakoon_interval
open Update
open Routing
open Client_cfg
open Ncfg
open Arakoon_client

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
  | REPLACE
  | REV_RANGE_ENTRIES
  | SYNCED_SEQUENCE
  | OPT_DB
  | DEFRAG_DB
  | DELETE_PREFIX
  | VERSION
  | DROP_MASTER
  | MULTI_GET_OPTION
  | CURRENT_STATE
  | NOP
  | FLUSH_STORE
  | GET_TXID
  | COPY_DB_TO_HEAD
  | USER_HOOK
  | LAST_ENTRIES3


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
  REPLACE                 , 0x33l;
  LAST_ENTRIES2           , 0x40l;
  NOP                     , 0x41l;
  FLUSH_STORE             , 0x42l;
  GET_TXID                , 0x43l;
  COPY_DB_TO_HEAD         , 0x44l;
  USER_HOOK               , 0x45l;
  LAST_ENTRIES3           , 0x46l;
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

let nothing = fun _ic -> Lwt.return ()

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

let consistency_to buffer = function
  | Consistent    -> Buffer.add_char buffer '\x00'
  | No_guarantees -> Buffer.add_char buffer '\x01'
  | At_least s    -> Buffer.add_char buffer '\x02';
                     Stamp.stamp_to buffer s

let input_consistency ic =
  Lwt_io.read_char ic >>=
    function
    | '\x00' -> Lwt.return Consistent
    | '\x01' -> Lwt.return No_guarantees
    | '\x02' -> Stamp.input_stamp ic >>= fun s -> Lwt.return (At_least s)
    |  c     -> failwith (Printf.sprintf "%C is not a consistency" c)
let output_consistency oc c =
  let b = Buffer.create 10 in
  consistency_to b c;
  Lwt_io.write oc (Buffer.contents b)

let consistency2s = function
  | Consistent -> "Consistent"
  | No_guarantees -> "No_guarantees"
  | At_least s -> Printf.sprintf "(At_least %s)" (Stamp.to_s s)


let exists_to buffer ~consistency key =
  command_to buffer EXISTS;
  consistency_to buffer consistency;
  Llio.string_to buffer key

let get_to ~consistency buffer key =
  command_to buffer GET;
  consistency_to buffer consistency;
  Llio.string_to buffer key

let assert_to ~consistency buffer key vo =
  command_to buffer ASSERT;
  consistency_to buffer consistency;
  Llio.string_to buffer key;
  Llio.string_option_to buffer vo

let assert_exists_to ~consistency buffer key=
  command_to buffer ASSERTEXISTS;
  consistency_to buffer consistency;
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

let range_to b ~consistency first finc last linc max =
  command_to b RANGE;
  consistency_to b consistency;
  Llio.string_option_to b first;
  Llio.bool_to b finc;
  Llio.string_option_to b last;
  Llio.bool_to b linc;
  Llio.int_to b max

let range_entries_to b ~consistency first finc last linc max =
  command_to b RANGE_ENTRIES;
  consistency_to b consistency;
  Llio.string_option_to b first;
  Llio.bool_to b finc;
  Llio.string_option_to b last;
  Llio.bool_to b linc;
  Llio.int_to b max

let rev_range_entries_to b ~consistency first finc last linc max =
  command_to b REV_RANGE_ENTRIES;
  consistency_to b consistency;
  Llio.string_option_to b first;
  Llio.bool_to b finc;
  Llio.string_option_to b last;
  Llio.bool_to b linc;
  Llio.int_to b max

let prefix_keys_to b ~consistency prefix max =
  command_to b PREFIX_KEYS;
  consistency_to b consistency;
  Llio.string_to b prefix;
  Llio.int_to b max

let test_and_set_to b key expected wanted =
  command_to b TEST_AND_SET;
  Llio.string_to b key;
  Llio.string_option_to b expected;
  Llio.string_option_to b wanted

let replace_to b key wanted =
  command_to b REPLACE;
  Llio.string_to b key;
  Llio.string_option_to b wanted

let user_function_to b name po =
  command_to b USER_FUNCTION;
  Llio.string_to b name;
  Llio.string_option_to b po

let user_hook_to b ~consistency name =
  command_to b USER_HOOK;
  consistency_to b consistency;
  Llio.string_to b name

let multiget_to b ~consistency keys =
  command_to b MULTI_GET;
  consistency_to b consistency;
  Llio.string_list_to b keys

let multiget_option_to b ~consistency keys =
  command_to b MULTI_GET_OPTION;
  consistency_to b consistency;
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

let nop (ic,oc) =
  request oc (fun buf -> command_to buf NOP) >>= fun () ->
  response ic nothing

let get_txid(ic,oc) =
  request oc (fun buf -> command_to buf GET_TXID) >>= fun () ->
  response ic input_consistency

let get (ic,oc) ~consistency key =
  request  oc (fun buf -> get_to ~consistency buf key) >>= fun () ->
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

let rec change_to_update c =
  match c with
  | Set (k,v) -> Update.Set(k,v)
  | Delete k -> Update.Delete k
  | TestAndSet (k,vo,v) -> Update.TestAndSet (k,vo,v)
  | Sequence cs -> Update.Sequence (List.map change_to_update cs)
  | Assert(k,vo) -> Update.Assert(k,vo)
  | Assert_exists k -> Update.Assert_exists k
  | Assert_range(p,a) -> Update.Assert_range(p,a)
  | Delete_prefix k  -> Update.DeletePrefix k
  | UserFunction(name,vo) -> Update.UserFunction(name, vo)

let _build_sequence_request buf changes =
  let update_buf = Buffer.create 100 in
  let updates = List.map change_to_update changes in
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
    Llio.input_string ic >>= fun s ->
    let buf = Llio.make_buffer s 0 in
    let cfg = NCFG.ncfg_from buf in
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

let copy_db_to_head (ic, oc) tlogs_to_keep =
  let outgoing buf =
    command_to buf COPY_DB_TO_HEAD;
    Llio.int_to buf tlogs_to_keep
  in
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
