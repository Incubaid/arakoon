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

open Std
open Common
open Lwt
open Log_extra
open Update
(*open Interval
open Routing
open Statistics
open Ncfg
open Client_cfg
*)

let section =
  let s = Logger.Section.make "client_protocol" in
  let () = Logger.Section.set_level s Logger.Debug in
  s

let handle_exceptions f =
    Lwt.catch
      f
      (fun exn ->
          let open Arakoon_protocol in
          begin match exn with
            | XException(e, msg) -> begin match e with
                | Arakoon_exc.E_NOT_FOUND ->
                    Lwt.return (Result.Not_found msg, false, Logger.Debug)
                | Arakoon_exc.E_ASSERTION_FAILED ->
                    Lwt.return (Result.Assertion_failed msg, false, Logger.Debug)
                | Arakoon_exc.E_NO_LONGER_MASTER ->
                    Lwt.return (Result.No_longer_master msg, false, Logger.Debug)
                | Arakoon_exc.E_GOING_DOWN ->
                    Lwt.return (Result.Going_down msg, true, Logger.Error)
                | Arakoon_exc.E_BAD_INPUT ->
                    Lwt.return (Result.Bad_input msg, false, Logger.Debug)
                | Arakoon_exc.E_INCONSISTENT_READ ->
                    Lwt.return (Result.Inconsistent_read msg, false, Logger.Debug)
                (* Unexpected things *)
                | Arakoon_exc.E_NO_MAGIC ->
                    Lwt.return (Result.No_magic msg, true, Logger.Error)
                | Arakoon_exc.E_NO_HELLO ->
                    Lwt.return (Result.No_hello msg, true, Logger.Error)
                | Arakoon_exc.E_NOT_MASTER ->
                    Lwt.return (Result.Not_master msg, true, Logger.Error)
                | Arakoon_exc.E_WRONG_CLUSTER ->
                    Lwt.return (Result.Wrong_cluster msg, true, Logger.Error)
                | Arakoon_exc.E_READ_ONLY ->
                    Lwt.return (Result.Read_only msg, true, Logger.Error)
                | Arakoon_exc.E_OUTSIDE_INTERVAL ->
                    Lwt.return (Result.Outside_interval msg, true, Logger.Error)
                | Arakoon_exc.E_NOT_SUPPORTED ->
                    Lwt.return (Result.Not_supported msg, true, Logger.Error)
                | Arakoon_exc.E_USERFUNCTION_FAILURE ->
                    Lwt.return (Result.Userfunction_failure msg, true, Logger.Error)
                | Arakoon_exc.E_MAX_CONNECTIONS ->
                    Lwt.return (Result.Max_connections msg, true, Logger.Error)
                | Arakoon_exc.E_UNKNOWN_FAILURE ->
                    Lwt.return (Result.Unknown_failure msg, true, Logger.Error)
                (* Very unexpected things *)
                | Arakoon_exc.E_OK ->
                    let msg' = Printf.sprintf "E_OK was raised unexpectedly: %s" msg in
                    Lwt.return (Result.Unknown_failure msg', true, Logger.Error)
            end
            | Not_found ->
                Lwt.return (Result.Not_found "Not_found", false, Logger.Debug)
            | Server.FOOBAR ->
                Lwt.return (Result.Unknown_failure "unknown failure", true, Logger.Error)
            | Canceled ->
                Lwt.fail Canceled
            | _ ->
                Lwt.return (Result.Unknown_failure "unknown failure", true, Logger.Error)
          end >>= fun (res, death, level) ->
          Logger.log_ section level (fun () ->
              Printf.sprintf
                  "Exception during client request (%s) => %s"
                  (Printexc.to_string exn)
                  (Arakoon_protocol.Result.to_string (fun _ -> "...") res)) >>= fun () ->
          if not death
              then Lwt.return (Rpc.Server.Result.Continue res)
              else Lwt.return (Rpc.Server.Result.Close res))

let decode_sequence update =
  begin
    match update with
      | Update.Sequence updates -> updates
      | Update.Set _
      | Update.Delete _
      | Update.TestAndSet _
      | Update.SyncedSequence _
      | Update.MasterSet _
      | Update.SetInterval _
      | Update.SetRouting _
      | Update.SetRoutingDelta _
      | Update.Nop
      | Update.Assert _
      | Update.Assert_exists _
      | Update.UserFunction _
      | Update.AdminSet _
      | Update.DeletePrefix _
      | Update.Replace _ ->  raise (XException (Arakoon_exc.E_UNKNOWN_FAILURE,
                                     "should have been a sequence"))
  end

let handle_sequence ~sync backend update =
    let updates = decode_sequence update in
    backend # sequence ~sync updates

open Arakoon_protocol

let ok v = Lwt.return (Rpc.Server.Result.Continue (Result.Ok v))


let collapse_tlogs id ic oc backend =
  let sw () = Int64.bits_of_float (Unix.gettimeofday()) in
  let t0 = sw() in
  let output_ok_value f oc n =
    let r = Arakoon_protocol.Result.Ok n in
    Arakoon_protocol.Result.to_channel (fun oc v ->
      Arakoon_protocol.Protocol.Type.to_channel oc f v) oc r >>= fun () ->
    Lwt_io.flush oc
  in
  let output_ok_int = output_ok_value Arakoon_protocol.Protocol.Type.int
  and output_ok_int64 = output_ok_value Arakoon_protocol.Protocol.Type.int64 in
  let cb' n =
    Logger.debug_f_ "CB' %i" n >>= fun () ->
    output_ok_int oc n
  in
  let cb  =
    let count = ref 0 in
    fun () ->
      Logger.debug_f_ "CB %i" !count >>= fun () ->
      let () = incr count in
      let ts = sw() in
      let d = Int64.sub ts t0 in
      output_ok_int64 oc d
  in
  Llio.input_int ic >>= fun n ->
  Logger.info_f_ "connection=%s COLLAPSE_TLOGS: n=%i" id n >>= fun () ->
  Logger.info_f_ "... Start collapsing ... (n=%i)" n >>= fun () ->
  backend # collapse n cb' cb >>= fun () ->
  Logger.info_ "... Finished collapsing ..." >>=
  ok

let handle_command :
  type r s. Backend.backend -> string -> (r, s) Protocol.t -> r -> s Rpc.Server.Result.t Lwt.t =
  fun backend id cmd req ->
  match cmd with
  | Protocol.Ping -> handle_exceptions (fun () ->
      let (client_id, cluster_id) = req in
      Logger.debug_f_ "connection=%s PING: client_id=%S cluster_id=%S" id client_id cluster_id >>= fun () ->
      backend # hello client_id cluster_id >>= ok)
  | Protocol.Flush_store -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s FLUSH_STORE" id >>= fun () ->
      backend # flush_store () >>= ok)
  | Protocol.Exists -> handle_exceptions (fun () ->
      let (consistency, key) = req in
      Logger.debug_f_ "connection=%s EXISTS: consistency=%s key=%S" id (consistency2s consistency) key >>= fun () ->
      let exists = backend # exists ~consistency key in
      ok exists)
  | Protocol.Get -> handle_exceptions (fun () ->
      let (consistency, key) = req in
      Logger.debug_f_ "connection=%s GET: consistency=%s key=%S" id (consistency2s consistency) key >>= fun () ->
      let value = backend # get ~consistency key in
      ok value)
  | Protocol.Assert -> handle_exceptions (fun () ->
      let (consistency, key, vo) = req in
      Logger.debug_f_ "connection=%s ASSERT: consistency=%s key=%S" id (consistency2s consistency) key >>= fun () ->
      let () = backend # aSSert ~consistency key vo in
      ok ())
  | Protocol.Assert_exists -> handle_exceptions (fun () ->
      let (consistency, key) = req in
      Logger.debug_f_ "connection=%s ASSERTEXISTS: consistency=%s key=%S" id (consistency2s consistency) key >>= fun () ->
      let () = backend # aSSert_exists ~consistency key in
      ok ())
  | Protocol.Set -> handle_exceptions (fun () ->
      let (key, value) = req in
      Logger.debug_f_ "connection=%s SET: key=%S" id key >>= fun () ->
      backend # set key value >>= ok)
  | Protocol.Nop -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s NOP" id >>= fun () ->
      backend # nop () >>= ok)
  | Protocol.Get_txid -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s GET_TXID" id >>= fun () ->
      let txid = backend # get_txid () in
      ok txid)
  | Protocol.Delete -> handle_exceptions (fun () ->
      let (key : string) = req in
      Logger.debug_f_ "connection=%s DELETE: key=%S" id key >>= fun () ->
      backend # delete key >>= ok)
  | Protocol.Range -> handle_exceptions (fun () ->
      let (consistency, rreq) = req in
      let open Arakoon_protocol.RangeRequest in
      let (first, finc, last, linc, max) = (rreq.first, rreq.finc, rreq.last, rreq.linc, rreq.max) in
      Logger.debug_f_ "connection=%s RANGE: consistency=%s first=%s finc=%B last=%s linc=%B max=%i"
        id (consistency2s consistency) (p_option first) finc (p_option last) linc max >>= fun () ->
      let keys = backend # range ~consistency first finc last linc max in
      ok (Arakoon_protocol.ReversedArray.make keys))
  | Protocol.Range_entries -> handle_exceptions (fun () ->
      let (consistency, rreq) = req in
      let open Arakoon_protocol.RangeRequest in
      let (first, finc, last, linc, max) = (rreq.first, rreq.finc, rreq.last, rreq.linc, rreq.max) in
      Logger.debug_f_ "connection=%s RANGE_ENTRIES: consistency=%s first=%s finc=%B last=%s linc=%B max=%i"
        id (consistency2s consistency) (p_option first) finc (p_option last) linc max >>= fun () ->
       let (length, list) = backend # range_entries ~consistency first finc last linc max in
       let l = Arakoon_protocol.CountedList.make ~length ~list in
       ok l)
  | Protocol.Rev_range_entries -> handle_exceptions (fun () ->
      let (consistency, rreq) = req in
      let open Arakoon_protocol.RangeRequest in
      let (first, finc, last, linc, max) = (rreq.first, rreq.finc, rreq.last, rreq.linc, rreq.max) in
      Logger.debug_f_ "connection=%s REV_RANGE_ENTRIES: consistency=%s first=%s finc=%B last=%s linc=%B max=%i"
        id (consistency2s consistency) (p_option first) finc (p_option last) linc max >>= fun () ->
      let (length, list) = backend # rev_range_entries ~consistency first finc last linc max in
      let l = Arakoon_protocol.CountedList.make ~length ~list in
      ok l)
(*  | LAST_ENTRIES ->
    begin
      Sn.input_sn ic >>= fun i ->
      Logger.debug_f_ "connection=%s LAST_ENTRIES: i=%Li" id i >>= fun () ->
      response_ok oc >>= fun () ->
      backend # last_entries i oc >>= fun () ->
      Lwt.return false
    end
  | LAST_ENTRIES2 ->
    begin
      Sn.input_sn ic >>= fun i ->
      Logger.debug_f_ "connection=%s LAST_ENTRIES2: i=%Li" id i >>= fun () ->
      response_ok oc >>= fun () ->
      backend # last_entries2 i oc >>= fun () ->
      Lwt.return false
    end*)
  | Protocol.Who_master -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s WHO_MASTER" id >>= fun () ->
      let m = backend # who_master () in
      ok m)
  | Protocol.Expect_progress_possible -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s EXPECT_PROGRESS_POSSIBLE" id >>= fun () ->
      let poss = backend # expect_progress_possible () in
      ok poss)
  | Protocol.Test_and_set -> handle_exceptions (fun () ->
      let (key, expected, wanted) = req in
      Logger.debug_f_ "connection=%s TEST_AND_SET: key=%S" id key >>= fun () ->
      backend # test_and_set key expected wanted >>= ok)
  | Protocol.Replace -> handle_exceptions (fun () ->
      let (key, wanted) = req in
      Logger.debug_f_ "connection=%s REPLACE: key=%S" id key >>= fun () ->
      backend # replace key wanted >>= ok)
  | Protocol.User_function -> handle_exceptions (fun () ->
      let (name, po) = req in
      Logger.debug_f_ "connection=%s USER_FUNCTION: name=%S" id name >>= fun () ->
      backend # user_function name po >>= ok)
  | Protocol.Prefix_keys -> handle_exceptions (fun () ->
      let (consistency, key, max) = req in
      Logger.debug_f_ "connection=%s PREFIX_KEYS: consistency=%s key=%S max=%i" id (consistency2s consistency) key max >>= fun () ->
      let (length, list) = backend # prefix_keys ~consistency key max in
      let l = CountedList.make ~length ~list in
      ok l)
  | Protocol.Multi_get -> handle_exceptions (fun () ->
      let (consistency, clist) = req in
      let length = CountedList.length clist
      and keys = CountedList.list clist in
      Logger.debug_f_ "connection=%s MULTI_GET: consistency=%s length=%i keys=%S" id (consistency2s consistency) length
        (String.concat ";" keys) >>= fun () ->
      let values = backend # multi_get ~consistency keys in
      ok values)
  | Protocol.Multi_get_option -> handle_exceptions (fun () ->
      let (consistency ,clist) = req in
      let length = CountedList.length clist
      and keys = CountedList.list clist in
      Logger.debug_f_ "connection=%s MULTI_GET_OPTION: consistency=%s length=%i keys=%S"
        id (consistency2s consistency) length (String.concat ";" keys) >>= fun () ->
      let vos = backend # multi_get_option ~consistency keys in
      ok vos)
  | Protocol.Sequence -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s SEQUENCE" id >>= fun () ->
      handle_sequence ~sync:false backend req >>= ok)
  | Protocol.Synced_sequence -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s SYNCED_SEQUENCE" id >>= fun () ->
      handle_sequence ~sync:true backend req >>= ok)
(*  | MIGRATE_RANGE ->
    begin
      wrap_exception
        (fun () ->
          Interval.input_interval ic >>= fun interval ->
          Logger.debug_f_ "connection=%s MIGRATE_RANGE" id
          >>= fun () ->
          decode_sequence ic >>= fun updates ->
          let interval_update = Update.SetInterval interval in
          let updates' =  interval_update :: updates in
          backend # sequence ~sync:false updates' >>= fun () ->
          response_ok_unit oc
        )
    end *)
  | Protocol.Statistics -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s STATISTICS" id >>= fun () ->
      let s = backend # get_statistics () in
      ok s)
  | Protocol.Copy_db_to_head -> handle_exceptions (fun () ->
      let tlogs_to_keep = req in
      Logger.debug_f_ "connection=%s COPY_DB_TO_HEAD: tlogs_to_keep=%i" id tlogs_to_keep >>= fun () ->
      backend # copy_db_to_head tlogs_to_keep >>= ok)
  (*| SET_INTERVAL ->
    begin
      wrap_exception
        (fun () ->
           Interval.input_interval ic >>= fun interval ->
           Logger.info_f_ "connection=%s SET_INTERVAL: interval %S" id (Interval.to_string interval) >>= fun () ->
           backend # set_interval interval >>= fun () ->
           response_ok_unit oc
        )
    end
  | GET_INTERVAL ->
    begin
      wrap_exception
        (fun() ->
          Logger.debug_f_ "connection=%s GET_INTERVAL" id >>= fun () ->
          let interval = backend # get_interval () in
          response_ok oc >>= fun () ->
          Interval.output_interval oc interval >>= fun () ->
          Lwt.return false
        )
    end
  | GET_ROUTING ->
     wrap_exception
       (fun () ->
         Logger.debug_f_ "connection=%s GET_ROUTING" id >>= fun () ->
         let routing = backend # get_routing () in
         response_ok oc >>= fun () ->
         Routing.output_routing oc routing >>= fun () ->
         Lwt.return false
      )
  | SET_ROUTING ->
    begin
      Routing.input_routing ic >>= fun routing ->
      Logger.info_f_ "connection=%s SET_ROUTING" id >>= fun () ->
      wrap_exception
        (fun () ->
           backend # set_routing routing >>= fun () ->
           response_ok_unit oc)
    end
  | SET_ROUTING_DELTA ->
    begin
      wrap_exception
        (fun () ->
          Llio.input_string ic >>= fun left ->
          Llio.input_string ic >>= fun sep ->
          Llio.input_string ic >>= fun right ->
          Logger.info_f_ "connection=%s SET_ROUTING_DELTA: left=%S sep=%S right=%S" id left sep right >>= fun () ->
          backend # set_routing_delta left sep right >>= fun () ->
          response_ok_unit oc )
    end*)
  | Protocol.Get_key_count -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s GET_KEY_COUNT" id >>= fun () ->
      let kc = backend # get_key_count () in
      ok kc)
  | Protocol.Optimize_db -> handle_exceptions (fun () ->
      Logger.info_f_ "connection=%s OPT_DB" id >>= fun () ->
      backend # optimize_db () >>= ok)
  | Protocol.Defrag_db -> handle_exceptions (fun () ->
      Logger.info_f_ "connection=%s DEFRAG_DB" id >>= fun () ->
      backend # defrag_db () >>= ok)
  | Protocol.Confirm -> handle_exceptions (fun () ->
      let (key, value) = req in
      Logger.debug_f_ "connection=%s CONFIRM: key=%S" id key >>= fun () ->
      backend # confirm key value >>= ok)
(*  | GET_NURSERY_CFG ->
    begin
      wrap_exception
        (fun () ->
          Logger.debug_f_ "connection=%s GET_NURSERY_CFG" id >>= fun () ->
          let routing = backend # get_routing () in
          let cfgs = backend # get_cluster_cfgs () in
          response_ok oc >>= fun () ->
          let buf = Buffer.create 32 in
          NCFG.ncfg_to buf (routing,cfgs);
          Llio.output_string oc (Buffer.contents buf) >>= fun () ->
          Lwt.return false
        )
    end
  | SET_NURSERY_CFG ->
    begin
      wrap_exception
        (fun () ->
          Llio.input_string ic >>= fun cluster_id ->
          ClientCfg.input_cfg ic >>= fun cfg ->
          Logger.info_f_ "connection=%s SET_NURSERY_CFG: cluster_id=%S" id cluster_id >>= fun () ->
          backend # set_cluster_cfg cluster_id cfg >>= fun () ->
          response_ok_unit oc
        )
    end
  | GET_FRINGE ->
    begin
      wrap_exception
        (fun () ->
           Llio.input_string_option ic >>= fun boundary ->
           Llio.input_int ic >>= fun dir_as_int ->
           let direction =
             if dir_as_int = 0
             then
               Routing.UPPER_BOUND
             else
               Routing.LOWER_BOUND
           in
           Logger.info_f_ "connection=%s GET_FRINGE: boundary=%s dir=%s"
             id
             (p_option boundary)
             (match direction with | Routing.UPPER_BOUND -> "UPPER_BOUND" | Routing.LOWER_BOUND -> "LOWER_BOUND")
           >>= fun () ->
           let kvs = backend # get_fringe boundary direction in
           Logger.debug_ "get_fringe backend op complete" >>= fun () ->
           response_ok oc >>= fun () ->
           Llio.output_counted_list Llio.output_key_value_pair oc kvs >>= fun () ->
           Logger.debug_ "get_fringe all done" >>= fun () ->
           Lwt.return false
        )
    end *)
  | Protocol.Delete_prefix -> handle_exceptions (fun () ->
      let prefix = req in
      Logger.debug_f_ "connection=%s DELETE_PREFIX %S" id prefix >>= fun () ->
      backend # delete_prefix prefix >>= ok)
  | Protocol.Version -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s VERSION" id >>= fun () ->
      let rest = Printf.sprintf "revision: %S\ncompiled: %S\nmachine: %S\n"
                   Arakoon_version.git_revision
                   Arakoon_version.compile_time
                   Arakoon_version.machine
      in
      ok (Arakoon_version.major,
          Arakoon_version.minor,
          Arakoon_version.patch,
          rest))
  | Protocol.Drop_master -> handle_exceptions (fun () ->
      Logger.info_f_ "connection=%s DROP_MASTER" id >>= fun () ->
      backend # drop_master () >>= ok)
  | Protocol.Current_state -> handle_exceptions (fun () ->
      Logger.debug_f_ "connection=%s CURRENT_STATE" id >>= fun () ->
      let state = backend # get_current_state () in
      ok state)

let protocol stop backend connection =
  let ic,oc,id = connection in
  let check magic version =
    if magic = _MAGIC && version = _VERSION then Lwt.return ()
    else Llio.lwt_failfmt "MAGIC %lx or VERSION %x mismatch" magic version
  in
  let check_cluster cluster_id =
    if backend # check ~cluster_id
    then Lwt.return ()
    else Llio.lwt_failfmt "WRONG CLUSTER: %s" cluster_id
  in
  let prologue () =
    Llio.input_int32  ic >>= fun magic ->
    Llio.input_int    ic >>= fun version ->
    check magic version  >>= fun () ->
    Llio.input_string ic >>= fun cluster_id ->
    check_cluster cluster_id >>= fun () ->
    Lwt.return ()
  in
  prologue () >>= fun () ->

  let module H = struct
    module Protocol = Arakoon_protocol.Protocol

    exception Unknown_tag of Protocol.Tag.t

    let handle : type r s. (r, s) Protocol.t -> r -> s Rpc.Server.Result.t Lwt.t = fun cmd req ->
        if !stop
        then begin
            Logger.debug_ "Leaving client loop" >>= fun () ->
            Lwt.return Rpc.Server.Result.Die
        end
        else
            handle_command backend id cmd req

    let handle_single_call_result r = begin match r with
        | Rpc.Server.Result.Continue res
        | Rpc.Server.Result.Close res -> begin
            match res with
              | Result.Ok () -> Lwt.return ()
              | Result.No_magic _
              | Result.No_hello _
              | Result.Not_master _
              | Result.Not_found _
              | Result.Wrong_cluster _
              | Result.Assertion_failed _
              | Result.Read_only _
              | Result.Outside_interval _
              | Result.Going_down _
              | Result.Not_supported _
              | Result.No_longer_master _
              | Result.Bad_input _
              | Result.Inconsistent_read _
              | Result.Userfunction_failure _
              | Result.Max_connections _
              | Result.Unknown_failure _ as res ->
                  Result.to_channel (fun _ () -> Lwt.return ()) oc res
        end
        | Rpc.Server.Result.Die -> Lwt.return ()
      end >>= fun () ->
      Lwt.return (Rpc.Server.Result.Die)


    let handle_unknown_tag oc = function
      | 0x1bl -> handle_exceptions (fun () ->
          Logger.info_f_ "connection=%s GET_DB" id >>= fun () ->
          backend # get_db (Some oc) >>= fun () ->
          ok ()) >>= handle_single_call_result
      | 0x14l -> handle_exceptions (fun () ->
          collapse_tlogs id ic oc backend) >>= handle_single_call_result
      | tag -> begin
          let m = Printf.sprintf "%lx: command not found" tag in
          let r = Result.Not_found m in
          Result.to_channel (fun _ _ -> Lwt.fail (Failure "Can't happen")) oc r >>= fun () ->
          Lwt.return (Rpc.Server.Result.Die)
      end

    let handle_exception (_ic, oc) = function
      | Unknown_tag tag -> handle_unknown_tag oc tag
      | Protocol.Tag.Invalid_magic _ as exn -> begin
          Logger.debug_ ~exn "Invalid magic" >>= fun () ->
          Arakoon_protocol.Result.to_channel
              (fun _ () -> Lwt.return ())
              oc
              (Arakoon_protocol.Result.no_magic "Invalid magic") >>= fun () ->
          Lwt.return (Rpc.Server.Result.Die)
      end
      | exn -> begin
          Logger.warning_ ~exn "Unknown failure" >>= fun () ->
          Arakoon_protocol.Result.to_channel
              (fun _ () -> Lwt.return ())
              oc
              (Arakoon_protocol.Result.unknown_failure "Unknown failure") >>= fun () ->
          Lwt.return (Rpc.Server.Result.Die)
      end

  end in

  let module S = Rpc.Server.Make(H) in

  S.session ic oc
