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

open Std
open Common
open Lwt
open Log_extra
open Extra
open Update
open Interval
open Routing
open Statistics
open Ncfg
open Client_cfg


let section =
  let s = Logger.Section.make "client_protocol" in
  let () = Logger.Section.set_level s Logger.Debug in
  s

let read_command (ic,oc) =
  Llio.input_int32 ic >>= fun masked ->
  let magic = Int32.logand masked _MAGIC in
  begin
    if magic <> _MAGIC
    then
      begin
        Llio.output_int32 oc 1l >>= fun () ->
        Llio.lwt_failfmt "%lx has no magic" masked
      end
    else
      begin
        let as_int32 = Int32.logand masked _MASK in
        try
          let c = lookup_code as_int32 in
          Lwt.return c
        with Not_found ->
          Llio.output_int32 oc 5l >>= fun () ->
          let msg = Printf.sprintf "%lx: command not found" as_int32 in
          Llio.output_string oc msg >>= fun () ->
          Lwt.fail (Failure msg)
      end
  end

let log_debug m =
  Logger.debug_ m

let response_ok oc =
  Llio.output_int32 oc (Arakoon_exc.int32_of_rc Arakoon_exc.E_OK)

let response_ok_unit oc =
  log_debug "ok_unit back to client" >>= fun () ->
  response_ok oc >>= fun () ->
  Lwt.return false

let response_ok_int64 oc i64 =
  response_ok oc >>= fun () ->
  Llio.output_int64 oc i64 >>= fun () ->
  Lwt.return false

let response_ok_string oc string =
  response_ok oc >>= fun () ->
  Llio.output_string oc string >>= fun () ->
  Lwt.return false

let response_ok_bool oc b =
  response_ok oc >>= fun () ->
  Llio.output_bool oc b >>= fun () ->
  Lwt.return false

let handle_exception oc exn=
  begin
    match exn with
      | XException(Arakoon_exc.E_NOT_FOUND, msg) ->
        Lwt.return (Arakoon_exc.E_NOT_FOUND,msg, false, false, Logger.Debug)
      | XException(Arakoon_exc.E_ASSERTION_FAILED, msg) ->
        Lwt.return (Arakoon_exc.E_ASSERTION_FAILED, msg, false, false, Logger.Debug)
      | XException(Arakoon_exc.E_NO_LONGER_MASTER, msg) ->
        Lwt.return (Arakoon_exc.E_NO_LONGER_MASTER, msg, false, false, Logger.Debug)
      | XException(Arakoon_exc.E_GOING_DOWN, msg) ->
        Lwt.return (Arakoon_exc.E_GOING_DOWN, msg, true, true, Logger.Error)
      | XException(rc, msg) ->
        Lwt.return (rc,msg, false, true, Logger.Error)
      | Not_found ->
        Lwt.return (Arakoon_exc.E_NOT_FOUND, "Not_found", false, false, Logger.Debug)
      | Server.FOOBAR ->
        Lwt.return (Arakoon_exc.E_UNKNOWN_FAILURE, "unkown failure", true, true, Logger.Error)
      | Canceled ->
        Lwt.fail Canceled
      | _ ->
        Lwt.return (Arakoon_exc.E_UNKNOWN_FAILURE, "unknown failure", false, true, Logger.Error)
  end
  >>= fun (rc, msg, is_fatal, close_socket, level) ->
  Logger.log_ section level
    (fun () -> Printf.sprintf
                 "Exception during client request (%s) => rc:%lx msg:%s"
                 (Printexc.to_string exn)  (Arakoon_exc.int32_of_rc rc) msg) >>= fun () ->
  Arakoon_exc.output_exception oc rc msg >>= fun () ->
  begin
    if close_socket
    then log_debug "Closing client socket" >>= fun () -> Lwt_io.close oc
    else Lwt.return ()
  end >>= fun () ->
  if is_fatal
  then Lwt.fail exn
  else Lwt.return close_socket


let decode_sequence ic =
  begin
    Llio.input_string ic >>= fun data ->
    Logger.debug_f_ "Read out %d bytes" (String.length data) >>= fun () ->
    let update,_ = Update.from_buffer data 0 in
    match update with
      | Update.Sequence updates ->
        Lwt.return updates
      | _ ->  raise (XException (Arakoon_exc.E_UNKNOWN_FAILURE,
                                 "should have been a sequence"))
  end

let handle_sequence ~sync ic oc backend =
  begin
    Lwt.catch
      (fun () ->
         begin
           decode_sequence ic >>= fun updates ->
           backend # sequence ~sync updates >>= fun () ->
           response_ok_unit oc
         end )
      ( handle_exception oc )

  end

let one_command stop (ic,oc,id) (backend:Backend.backend) =
  read_command (ic,oc) >>= fun command ->
  if !stop
  then
    Lwt.return true
  else
    match command with
  | PING ->
     begin
       Lwt.catch
         (fun () ->
          Llio.input_string ic >>= fun client_id ->
          Llio.input_string ic >>= fun cluster_id ->
          Logger.debug_f_ "connection=%s PING: client_id=%S cluster_id=%S" id client_id cluster_id >>= fun () ->
          backend # hello client_id cluster_id >>= fun msg ->
          response_ok_string oc msg)
         (handle_exception oc)
    end
  | FLUSH_STORE ->
     begin
       Lwt.catch
         (fun () ->
          Logger.debug_f_ "connection=%s FLUSH_STORE" id >>= fun () ->
          backend # flush_store () >>= fun () ->
          response_ok_unit oc)
         (handle_exception oc)
     end
  | EXISTS ->
    begin
      Common.input_consistency ic  >>= fun consistency ->
      Llio.input_string ic >>= fun key ->
      Logger.debug_f_ "connection=%s EXISTS: consistency=%s key=%S" id (consistency2s consistency) key >>= fun () ->
      Lwt.catch
        (fun () -> backend # exists ~consistency key >>= fun exists ->
                   response_ok_bool oc exists)
        (handle_exception oc)
    end
  | GET ->
    begin
      Common.input_consistency   ic >>= fun consistency ->
      Llio.input_string ic >>= fun  key ->
      Logger.debug_f_ "connection=%s GET: consistency=%s key=%S" id (consistency2s consistency) key >>= fun () ->
      Lwt.catch
        (fun () -> backend # get ~consistency key >>= fun value ->
          response_ok_string oc value)
        (handle_exception oc)
    end
  | ASSERT ->
    begin
      Common.input_consistency ic >>= fun consistency ->
      Llio.input_string ic        >>= fun key ->
      Llio.input_string_option ic >>= fun vo ->
      Logger.debug_f_ "connection=%s ASSERT: consistency=%s key=%S" id (consistency2s consistency) key >>= fun () ->
      Lwt.catch
        (fun () -> backend # aSSert ~consistency key vo >>= fun () ->
          response_ok_unit oc
        )
        (handle_exception oc)
    end
  | ASSERTEXISTS ->
    begin
      Common.input_consistency ic >>= fun consistency ->
      Llio.input_string ic        >>= fun key ->
      Logger.debug_f_ "connection=%s ASSERTEXISTS: consistency=%s key=%S" id (consistency2s consistency) key >>= fun () ->
      Lwt.catch
        (fun () -> backend # aSSert_exists ~consistency key>>= fun () ->
          response_ok_unit oc
        )
        (handle_exception oc)
    end
  | SET ->
    begin
      Llio.input_string ic >>= fun key ->
      Llio.input_string ic >>= fun value ->
      Logger.debug_f_ "connection=%s SET: key=%S" id key >>= fun () ->
      Lwt.catch
        (fun () -> backend # set key value >>= fun () ->
          response_ok_unit oc
        )
        (handle_exception oc)
    end
  | NOP ->
      begin
        Logger.debug_f_ "connection=%s NOP" id >>= fun () ->
        Lwt.catch
          (fun () ->
           backend # nop () >>= fun () ->
           response_ok_unit oc)
          (handle_exception oc)
      end
  | DELETE ->
    begin
      Llio.input_string ic >>= fun key ->
      Logger.debug_f_ "connection=%s DELETE: key=%S" id key >>= fun () ->
      Lwt.catch
        (fun () ->
           backend # delete key >>= fun () ->
           response_ok_unit oc)
        (handle_exception oc)
    end
  | RANGE ->
    begin
      Common.input_consistency ic >>= fun consistency ->
      Llio.input_string_option ic >>= fun (first:string option) ->
      Llio.input_bool          ic >>= fun finc  ->
      Llio.input_string_option ic >>= fun (last:string option)  ->
      Llio.input_bool          ic >>= fun linc  ->
      Llio.input_int           ic >>= fun max   ->
      Logger.debug_f_ "connection=%s RANGE: consistency=%s first=%s finc=%B last=%s linc=%B max=%i"
        id (consistency2s consistency) (p_option first) finc (p_option last) linc max >>= fun () ->
      Lwt.catch
        (fun () ->
          backend # range ~consistency first finc last linc max >>= fun keys ->
          response_ok oc >>= fun () ->
          Llio.output_string_array_reversed oc keys >>= fun () ->
          Lwt.return false
        )
        (handle_exception oc )
    end
  | RANGE_ENTRIES ->
    begin
      Common.input_consistency ic >>= fun consistency ->
      Llio.input_string_option ic >>= fun first ->
      Llio.input_bool          ic >>= fun finc  ->
      Llio.input_string_option ic >>= fun last  ->
      Llio.input_bool          ic >>= fun linc  ->
      Llio.input_int           ic >>= fun max   ->
      Logger.debug_f_ "connection=%s RANGE_ENTRIES: consistency=%s first=%s finc=%B last=%s linc=%B max=%i"
        id (consistency2s consistency) (p_option first) finc (p_option last) linc max >>= fun () ->
      Lwt.catch
        (fun () ->
           backend # range_entries ~consistency first finc last linc max
           >>= fun (kvs:(string*string) counted_list) ->
           response_ok oc >>= fun () ->
           Llio.output_counted_list Llio.output_string_pair oc kvs >>= fun () ->
           Lwt.return false
        )
        (handle_exception oc)
    end
  | REV_RANGE_ENTRIES ->
    begin
      Common.input_consistency ic >>= fun consistency ->
      Llio.input_string_option ic >>= fun first ->
      Llio.input_bool          ic >>= fun finc  ->
      Llio.input_string_option ic >>= fun last  ->
      Llio.input_bool          ic >>= fun linc  ->
      Llio.input_int           ic >>= fun max   ->
      Logger.debug_f_ "connection=%s REV_RANGE_ENTRIES: consistency=%s first=%s finc=%B last=%s linc=%B max=%i"
        id (consistency2s consistency) (p_option first) finc (p_option last) linc max >>= fun () ->
      Lwt.catch
        (fun () ->
           backend # rev_range_entries ~consistency first finc last linc max
           >>= fun (kvs:(string*string) counted_list) ->
           response_ok oc >>= fun () ->
           Llio.output_counted_list Llio.output_string_pair oc kvs >>= fun () ->
           Lwt.return false
        )
        (handle_exception oc)
    end
  | LAST_ENTRIES ->
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
    end
  | WHO_MASTER ->
    begin
      Logger.debug_f_ "connection=%s WHO_MASTER" id >>= fun () ->
      backend # who_master () >>= fun m ->
      response_ok oc >>= fun () ->
      Llio.output_string_option oc m >>= fun () ->
      Lwt.return false
    end
  | EXPECT_PROGRESS_POSSIBLE ->
    begin
      Logger.debug_f_ "connection=%s EXPECT_PROGRESS_POSSIBLE" id >>= fun () ->
      backend # expect_progress_possible () >>= fun poss ->
      response_ok_bool oc poss
    end
  | TEST_AND_SET ->
    begin
      Llio.input_string ic >>= fun key ->
      Llio.input_string_option ic >>= fun expected ->
      Llio.input_string_option ic >>= fun wanted ->
      Logger.debug_f_ "connection=%s TEST_AND_SET: key=%S" id key >>= fun () ->
      backend # test_and_set key expected wanted >>= fun vo ->
      response_ok oc >>= fun () ->
      Llio.output_string_option oc vo >>= fun () ->
      Lwt.return false
    end
  | REPLACE ->
     begin
       Lwt.catch
         (fun () ->
          Llio.input_string ic >>= fun key ->
          Llio.input_string_option ic >>= fun wanted ->
          Logger.debug_f_ "connection=%s REPLACE: key=%S" id key >>= fun () ->
          backend # replace key wanted >>= fun vo ->
          response_ok oc >>= fun () ->
          Llio.output_string_option oc vo >>= fun () ->
          Lwt.return false)
         (handle_exception oc)
     end
  | USER_FUNCTION ->
    begin
      Llio.input_string ic >>= fun name ->
      Llio.input_string_option ic >>= fun po ->
      Logger.debug_f_ "connection=%s USER_FUNCTION: name=%S" id name
      >>= fun () ->
      Lwt.catch
        (fun () ->
           begin
             backend # user_function name po >>= fun ro ->
             response_ok oc >>= fun () ->
             Llio.output_string_option oc ro >>= fun () ->
             Lwt.return false
           end
        )
        (handle_exception oc)
    end
  | PREFIX_KEYS ->
    begin
      Common.input_consistency ic >>= fun consistency ->
      Llio.input_string ic >>= fun key ->
      Llio.input_int    ic >>= fun max ->
      Logger.debug_f_ "connection=%s PREFIX_KEYS: consistency=%s key=%S max=%i" id (consistency2s consistency) key max
      >>= fun () ->
      backend # prefix_keys ~consistency key max >>= fun keys ->
      response_ok oc >>= fun () ->
      Llio.output_counted_list Llio.output_string oc keys >>= fun () ->
      Lwt.return false
    end
  | MULTI_GET ->
    begin
      Common.input_consistency ic >>= fun consistency ->
      Llio.input_listl Llio.input_string ic >>= fun (length, keys) ->
      Logger.debug_f_ "connection=%s MULTI_GET: consistency=%s length=%i keys=%S" id (consistency2s consistency) length
        (String.concat ";" keys) >>= fun () ->
      Lwt.catch
        (fun () ->
           backend # multi_get ~consistency keys >>= fun values ->
           response_ok oc >>= fun () ->
           Llio.output_string_list oc values >>= fun () ->
           Lwt.return false
        )
        (handle_exception oc)
    end
  | MULTI_GET_OPTION ->
    begin
      Common.input_consistency ic >>= fun consistency ->
      Llio.input_listl Llio.input_string ic >>= fun (length, keys) ->
      Logger.debug_f_ "connection=%s MULTI_GET_OPTION: consistency=%s length=%i keys=%S"
        id (consistency2s consistency) length (String.concat ";" keys) >>= fun () ->
      Lwt.catch
        (fun () ->
           backend # multi_get_option ~consistency keys >>= fun vos ->

           response_ok oc >>= fun () ->
           Llio.output_list Llio.output_string_option oc (List.rev vos) >>= fun () ->
           Lwt.return false)
        (handle_exception oc)
    end
  | SEQUENCE ->
    Logger.debug_f_ "connection=%s SEQUENCE" id >>= fun () ->
    handle_sequence ~sync:false ic oc backend
  | SYNCED_SEQUENCE ->
    Logger.debug_f_ "connection=%s SYNCED_SEQUENCE" id >>= fun () ->
    handle_sequence ~sync:true ic oc backend
  | MIGRATE_RANGE ->
    begin
      Lwt.catch(
        fun () ->
          Interval.input_interval ic >>= fun interval ->
          Logger.debug_f_ "connection=%s MIGRATE_RANGE" id
          >>= fun () ->
          decode_sequence ic >>= fun updates ->
          let interval_update = Update.SetInterval interval in
          let updates' =  interval_update :: updates in
          backend # sequence ~sync:false updates' >>= fun () ->
          response_ok_unit oc
      ) (handle_exception oc)
    end
  | STATISTICS ->
    begin
      Logger.debug_f_ "connection=%s STATISTICS" id
      >>= fun () ->
      let s = backend # get_statistics () in
      response_ok oc >>= fun () ->
      let b = Buffer.create 100 in
      Statistics.to_buffer b s;
      let bs = Buffer.contents b in
      Llio.output_string oc bs >>= fun () ->
      Lwt.return false
    end
  | COLLAPSE_TLOGS ->
    begin
      let sw () = Int64.bits_of_float (Unix.gettimeofday()) in
      let t0 = sw() in
      let cb' n =
        Logger.debug_f_ "CB' %i" n >>= fun () ->
        response_ok oc >>= fun () ->
        Llio.output_int oc n >>= fun () ->
        Lwt_io.flush oc
      in
      let cb  =
        let count = ref 0 in
        fun () ->
          Logger.debug_f_ "CB %i" !count >>= fun () ->
          let () = incr count in
          let ts = sw() in
          let d = Int64.sub ts t0 in
          response_ok oc >>= fun () ->
          Llio.output_int64 oc d >>= fun () ->
          Lwt_io.flush oc
      in
      Llio.input_int ic >>= fun n ->
      Logger.info_f_ "connection=%s COLLAPSE_TLOGS: n=%i" id n >>= fun () ->
      Lwt.catch
        (fun () ->
           Logger.info_f_ "... Start collapsing ... (n=%i)" n >>= fun () ->
           backend # collapse n cb' cb >>= fun () ->
           Logger.info_ "... Finished collapsing ..." >>= fun () ->
           Lwt.return false
        )
        (handle_exception oc)
    end
  | SET_INTERVAL ->
    begin
      Lwt.catch
        (fun () ->
           Interval.input_interval ic >>= fun interval ->
           Logger.info_f_ "connection=%s SET_INTERVAL: interval %S" id (Interval.to_string interval) >>= fun () ->
           backend # set_interval interval >>= fun () ->
           response_ok_unit oc
        )
        (handle_exception oc)
    end
  | GET_INTERVAL ->
    begin
      Lwt.catch(
        fun() ->
          Logger.debug_f_ "connection=%s GET_INTERVAL" id >>= fun () ->
          backend # get_interval () >>= fun interval ->
          response_ok oc >>= fun () ->
          Interval.output_interval oc interval >>= fun () ->
          Lwt.return false
      )
        (handle_exception oc)
    end
  | GET_ROUTING ->
    Lwt.catch
      (fun () ->
         Logger.debug_f_ "connection=%s GET_ROUTING" id >>= fun () ->
         backend # get_routing () >>= fun routing ->
         response_ok oc >>= fun () ->
         Routing.output_routing oc routing >>= fun () ->
         Lwt.return false
      )
      (handle_exception oc)
  | SET_ROUTING ->
    begin
      Routing.input_routing ic >>= fun routing ->
      Logger.info_f_ "connection=%s SET_ROUTING" id >>= fun () ->
      Lwt.catch
        (fun () ->
           backend # set_routing routing >>= fun () ->
           response_ok_unit oc)
        (handle_exception oc)
    end
  | SET_ROUTING_DELTA ->
    begin
      Lwt.catch(
        fun () ->
          Llio.input_string ic >>= fun left ->
          Llio.input_string ic >>= fun sep ->
          Llio.input_string ic >>= fun right ->
          Logger.info_f_ "connection=%s SET_ROUTING_DELTA: left=%S sep=%S right=%S" id left sep right >>= fun () ->
          backend # set_routing_delta left sep right >>= fun () ->
          response_ok_unit oc )
        (handle_exception oc)
    end
  | GET_KEY_COUNT ->
    begin
      Lwt.catch
        (fun() ->
           Logger.debug_f_ "connection=%s GET_KEY_COUNT" id >>= fun () ->
           backend # get_key_count () >>= fun kc ->
           response_ok_int64 oc kc)
        (handle_exception oc)
    end
  | GET_DB ->
    begin
      Lwt.catch
        (fun() ->
           Logger.info_f_ "connection=%s GET_DB" id >>= fun () ->
           backend # get_db (Some oc) >>= fun () ->
           Lwt.return false
        )
        (handle_exception oc)
    end
  | OPT_DB ->
    begin
      Lwt.catch
        ( fun () ->
           Logger.info_f_ "connection=%s OPT_DB" id >>= fun () ->
           backend # optimize_db () >>= fun () ->
           response_ok_unit oc
        )
        (handle_exception oc)
    end
  | DEFRAG_DB ->
    begin
      Lwt.catch
        (fun () ->
           Logger.info_f_ "connection=%s DEFRAG_DB" id >>= fun () ->
           backend # defrag_db () >>= fun () ->
           response_ok_unit oc)
        (handle_exception oc)
    end
  | CONFIRM ->
    begin
      Llio.input_string ic >>= fun key ->
      Llio.input_string ic >>= fun value ->
      Lwt.catch
        (fun () ->
           Logger.debug_f_ "connection=%s CONFIRM: key=%S" id key >>= fun () ->
           backend # confirm key value >>= fun () ->
           response_ok_unit oc
        )
        (handle_exception oc)
    end
  | GET_NURSERY_CFG ->
    begin
      Lwt.catch (
        fun () ->
          Logger.debug_f_ "connection=%s GET_NURSERY_CFG" id >>= fun () ->
          backend # get_routing () >>= fun routing ->
          backend # get_cluster_cfgs () >>= fun cfgs ->
          response_ok oc >>= fun () ->
          let buf = Buffer.create 32 in
          NCFG.ncfg_to buf (routing,cfgs);
          Llio.output_string oc (Buffer.contents buf) >>= fun () ->
          Lwt.return false
      )
        ( handle_exception oc )
    end
  | SET_NURSERY_CFG ->
    begin
      Lwt.catch (
        fun () ->
          Llio.input_string ic >>= fun cluster_id ->
          ClientCfg.input_cfg ic >>= fun cfg ->
          Logger.info_f_ "connection=%s SET_NURSERY_CFG: cluster_id=%S" id cluster_id >>= fun () ->
          backend # set_cluster_cfg cluster_id cfg >>= fun () ->
          response_ok_unit oc
      )
        ( handle_exception oc )
    end
  | GET_FRINGE ->
    begin
      Lwt.catch
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
           backend # get_fringe boundary direction >>= fun kvs ->
           Logger.debug_ "get_fringe backend op complete" >>= fun () ->
           response_ok oc >>= fun () ->
           Llio.output_counted_list Llio.output_string_pair oc kvs >>= fun () ->
           Logger.debug_ "get_fringe all done" >>= fun () ->
           Lwt.return false
        )
        (handle_exception oc)
    end
  | DELETE_PREFIX ->
    begin
      Lwt.catch
        ( fun () ->
           Llio.input_string ic >>= fun prefix ->
           Logger.debug_f_ "connection=%s DELETE_PREFIX %S" id prefix >>= fun () ->
           backend # delete_prefix prefix >>= fun n_deleted ->
           response_ok oc >>= fun () ->
           Llio.output_int oc n_deleted >>= fun () ->
           Lwt.return false
        )
        (handle_exception oc)
    end
  | VERSION ->
    begin
      Logger.debug_f_ "connection=%s VERSION" id >>= fun () ->
      response_ok oc >>= fun () ->
      Llio.output_int oc Arakoon_version.major >>= fun () ->
      Llio.output_int oc Arakoon_version.minor >>= fun () ->
      Llio.output_int oc Arakoon_version.patch >>= fun () ->
      let rest = Printf.sprintf "revision: %S\ncompiled: %S\nmachine: %S\n"
                   Arakoon_version.git_revision
                   Arakoon_version.compile_time
                   Arakoon_version.machine
      in
      Llio.output_string oc rest >>= fun () ->
      Lwt.return false
    end
  | DROP_MASTER ->
    begin
      Logger.info_f_ "connection=%s DROP_MASTER" id >>= fun () ->
      Lwt.catch
        (fun () -> backend # drop_master () >>= fun () ->
          response_ok_unit oc)
        (handle_exception oc)
    end
  | CURRENT_STATE ->
    begin
      Logger.debug_f_ "connection=%s CURRENT_STATE" id >>= fun () ->
      backend # get_current_state () >>= fun state ->
      response_ok oc >>= fun () ->
      Llio.output_string oc state >>= fun () ->
      Lwt.return false
    end


let protocol stop backend connection =
  let ic,oc,cid = connection in
  let check magic version =
    if magic = _MAGIC && version = _VERSION then Lwt.return ()
    else Llio.lwt_failfmt "MAGIC %lx or VERSION %x mismatch" magic version
  in
  let check_cluster cluster_id =
    backend # check ~cluster_id >>= fun ok ->
    if ok then Lwt.return ()
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
  let rec loop () =
    begin
      one_command stop connection backend >>= fun closed ->
      Lwt_io.flush oc >>= fun() ->
      if closed || !stop
      then Logger.debug_ "leaving client loop"
      else loop ()
    end
  in
  prologue () >>= fun () ->
  loop ()
