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

open Common
open Lwt
open Lwt_log
open Log_extra
open Extra
open Update
open Interval
open Routing
open Statistics
open Ncfg
open Client_cfg

let _s_ = string_option2s
let section = Lwt_log.Section.make "client_protocol"
(*let () = Lwt_log.Section.set_level section Lwt_log.Debug*)

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


let response_ok_unit cid oc =
  Lwt_log.debug_f ~section  "[connection:%i] ok_unit back to client" cid >>= fun () ->
  Llio.output_int32 oc 0l >>= fun () ->
  Lwt.return false

let response_ok_int64 cid oc i64 =
  Lwt_log.debug_f ~section "[connection:%i] ok_int64 back to client" cid >>= fun () ->
  Llio.output_int32 oc 0l >>= fun () ->
  Llio.output_int64 oc i64 >>= fun () ->
  Lwt.return false

let response_rc_string cid oc rc string =
  Lwt_log.debug_f ~section "[connection:%i] ok_rc back to client" cid >>= fun () ->
  Llio.output_int32 oc rc >>= fun () ->
  Llio.output_string oc string >>= fun () ->
  Lwt.return false

let response_rc_bool cid oc rc b =
  Lwt_log.debug_f ~section "[connection:%i] ok_rc_bool back to client" cid >>= fun () ->
  Llio.output_int32 oc rc >>= fun () ->
  Llio.output_bool oc b >>= fun () ->
  Lwt.return false

let handle_exception cid oc exn=
  let rc, msg, is_fatal, close_socket = match exn with
  | XException(Arakoon_exc.E_NOT_FOUND, msg) -> Arakoon_exc.E_NOT_FOUND,msg, false, false
  | XException(Arakoon_exc.E_GOING_DOWN, msg) ->Arakoon_exc.E_GOING_DOWN, msg, true, true
  | XException(Arakoon_exc.E_ASSERTION_FAILED, msg) ->
    Arakoon_exc.E_ASSERTION_FAILED, msg, false, false
  | XException(rc, msg) -> rc,msg, false, true
  | Not_found -> Arakoon_exc.E_NOT_FOUND, "Not_found", false, false
  | Server.FOOBAR -> Arakoon_exc.E_UNKNOWN_FAILURE, "unkown failure", true, true
  | _ -> Arakoon_exc.E_UNKNOWN_FAILURE, "unknown failure", false, true
  in
  Lwt_log.error_f "[connection:%i] Exception during client request (%s) => rc:%lx msg:%s"cid 
    (Printexc.to_string exn)  (Arakoon_exc.int32_of_rc rc) msg
  >>= fun () ->
  
  Arakoon_exc.output_exception oc rc msg >>= fun () ->
  begin
	  if close_socket
	  then Lwt_log.debug_f ~section "[connection:%i] Closing client socket" cid >>= fun () -> Lwt_io.close oc
	  else Lwt.return ()
  end >>= fun () ->
  if is_fatal
  then Lwt.fail exn
  else Lwt.return close_socket


let decode_sequence cid ic =
  begin
    Llio.input_string ic >>= fun data ->
    Lwt_log.debug_f ~section "[connection:%i] Read out %d bytes" cid (String.length data) >>= fun () ->
    let update,_ = Update.from_buffer data 0 in
    match update with
      | Update.Sequence updates ->
        Lwt.return updates
      | _ ->  raise (XException (Arakoon_exc.E_UNKNOWN_FAILURE,
             "[connection]:"^(string_of_int cid)^" should have been a sequence"))
  end

let handle_sequence cid ~sync ic oc backend =
  begin
    Lwt.catch
      (fun () ->
        begin
          decode_sequence cid ic >>= fun updates ->
          backend # sequence ~sync updates >>= fun () ->
          response_ok_unit cid oc
        end )
      (handle_exception cid oc)

  end

let one_command (ic,oc,cid) (backend:Backend.backend) =
  read_command (ic,oc) >>= function
    | PING ->
      begin
        Llio.input_string ic >>= fun client_id ->
	    Llio.input_string ic >>= fun cluster_id ->
        backend # hello client_id cluster_id >>= fun (rc,msg) ->
        Lwt_log.info_f "[connection:%i] PING, client:%s, cluster:%s" cid client_id cluster_id >>= fun () ->
        response_rc_string cid oc rc msg
      end
    | EXISTS ->
      begin
	Llio.input_bool ic   >>= fun allow_dirty ->
	Llio.input_string ic >>= fun key ->
	Lwt.catch
	  (fun () -> backend # exists ~allow_dirty key >>= fun exists ->
        Lwt_log.debug_f ~section "[connection:%i] Exists, key: %s" cid key >>= fun () ->
	    response_rc_bool cid oc 0l exists)
	  (handle_exception cid oc)
      end
    | GET ->
      begin
	    Llio.input_bool   ic >>= fun allow_dirty ->
        Llio.input_string ic >>= fun  key ->
	    Lwt.catch
	      (fun () -> backend # get ~allow_dirty key >>= fun value ->
            Lwt_log.debug_f ~section "[connection:%i] Get, key: %s" cid key >>= fun () ->
	        response_rc_string cid oc 0l value)
	      (handle_exception cid oc)
      end
    | ASSERT ->
      begin
	Llio.input_bool ic          >>= fun allow_dirty ->
	Llio.input_string ic        >>= fun key ->
	Llio.input_string_option ic >>= fun vo ->
	Lwt.catch
	  (fun () -> backend # aSSert ~allow_dirty key vo >>= fun () ->
        Lwt_log.debug_f ~section "[connection:%i] Assert, Key:%s" cid key >>= fun () ->
	    response_ok_unit cid oc
	  )
	  (handle_exception cid oc)
      end
    | ASSERTEXISTS ->
      begin
	Llio.input_bool ic          >>= fun allow_dirty ->
	Llio.input_string ic        >>= fun key ->
	Lwt.catch
	  (fun () -> backend # aSSert_exists ~allow_dirty key>>= fun () ->
        Lwt_log.debug_f ~section "[connection:%i] Assert_exists, Key:%s" cid key >>= fun () ->
	    response_ok_unit cid oc
	  )
	  (handle_exception cid oc)
      end
    | SET ->
	begin
      Llio.input_string ic >>= fun key ->
      Llio.input_string ic >>= fun value ->
	  Lwt.catch
	    (fun () -> backend # set key value >>= fun () ->
          Lwt_log.debug_f ~section "[connection:%i] SET, key:%s, Value:%s" cid key value >>= fun () ->
	      response_ok_unit cid oc
	    )
	    (handle_exception cid oc)
	end
    | DELETE ->
	begin
      Llio.input_string ic >>= fun key ->
      Lwt.catch
	    (fun () ->
	      backend # delete key >>= fun () ->
          Lwt_log.debug_f ~section "[connection:%i] Delete, key:%s" cid key >>= fun () ->
	      response_ok_unit cid oc)
	    (handle_exception cid oc)
	end
    | RANGE ->
      begin
	    Llio.input_bool ic >>= fun allow_dirty ->
        Llio.input_string_option ic >>= fun (first:string option) ->
        Llio.input_bool          ic >>= fun finc  ->
        Llio.input_string_option ic >>= fun (last:string option)  ->
        Llio.input_bool          ic >>= fun linc  ->
        Llio.input_int           ic >>= fun max   ->
        Lwt.catch
	      (fun () ->
	        backend # range ~allow_dirty first finc last linc max >>= fun list ->
            Lwt_log.debug_f ~section "[connection: %i] range %s %b %s %b %i" cid (_s_ first) finc (_s_ last) linc max >>= fun () ->
	        Llio.output_int32 oc 0l >>= fun () ->
	        Llio.output_list Llio.output_string oc list >>= fun () ->
            Lwt.return false
	      )
	      (handle_exception cid oc )
	end
    | RANGE_ENTRIES ->
      begin
	    Llio.input_bool          ic >>= fun allow_dirty ->
	    Llio.input_string_option ic >>= fun first ->
	    Llio.input_bool          ic >>= fun finc  ->
	    Llio.input_string_option ic >>= fun last  ->
	    Llio.input_bool          ic >>= fun linc  ->
	    Llio.input_int           ic >>= fun max   ->
        Lwt.catch
	      (fun () ->
	        backend # range_entries ~allow_dirty first finc last linc max
	        >>= fun (list:(string*string) list) ->
            Lwt_log.debug_f ~section "[connection: %i] range_entries %s %b %s %b %i" cid (_s_ first) finc (_s_ last) linc max >>= fun () ->
	        Llio.output_int32 oc 0l >>= fun () ->
	        let size = List.length list in
	        Lwt_log.debug_f ~section "size = %i" size >>= fun () ->
	        Llio.output_list Llio.output_string_pair oc list >>= fun () ->
            Lwt.return false
	      )
	      (handle_exception cid oc)
      end
    | REV_RANGE_ENTRIES ->
      begin
	    Llio.input_bool          ic >>= fun allow_dirty ->
	    Llio.input_string_option ic >>= fun first ->
	    Llio.input_bool          ic >>= fun finc  ->
	    Llio.input_string_option ic >>= fun last  ->
	    Llio.input_bool          ic >>= fun linc  ->
	    Llio.input_int           ic >>= fun max   ->
        Lwt.catch
	      (fun () ->
	        backend # rev_range_entries ~allow_dirty first finc last linc max
	        >>= fun (list:(string*string) list) ->
            Lwt_log.debug_f ~section "[connection: %i] rev_range_entries %s %b %s %b %i" cid (_s_ first) finc (_s_ last) linc max >>= fun () ->
	        Llio.output_int32 oc 0l >>= fun () ->
	        let size = List.length list in
	        Lwt_log.debug_f ~section "[connection: %i] size = %i" cid size >>= fun () ->
	        Llio.output_list Llio.output_string_pair oc list >>= fun () ->
            Lwt.return false
	      )
	      (handle_exception cid oc)
      end
    | LAST_ENTRIES ->
      begin
	    Sn.input_sn ic >>= fun i ->
	    Llio.output_int32 oc 0l >>= fun () ->
	    backend # last_entries i oc >>= fun () ->
        Lwt_log.debug_f ~section "[connection:%i] LAST_EXTRIES, i:%s" cid (Sn.string_of i) >>= fun () ->
        Lwt.return false
      end
    | WHO_MASTER ->
      begin
	    backend # who_master () >>= fun m ->
        Lwt_log.debug_f ~section "[connection:%i] WHO_MASTER, returns:%s" cid (_s_ m) >>= fun () ->
	    Llio.output_int32 oc 0l >>= fun () ->
	    Llio.output_string_option oc m >>= fun () ->
	    Lwt.return false
      end
    | EXPECT_PROGRESS_POSSIBLE ->
      begin
	    backend # expect_progress_possible () >>= fun poss ->
        Lwt_log.debug_f ~section "[connection:%i] EXPECT_PROGRESS_POSSIBLE" cid >>= fun () ->
	    Llio.output_int32 oc 0l >>= fun () ->
	    Llio.output_bool oc poss >>= fun () ->
	    Lwt.return false
      end
    | TEST_AND_SET ->
      begin
	    Llio.input_string ic >>= fun key ->
	    Llio.input_string_option ic >>= fun expected ->
        Llio.input_string_option ic >>= fun wanted ->
	    backend # test_and_set key expected wanted >>= fun vo ->
        Lwt_log.debug_f ~section "[connection:%i] TEST_AND_SET, key:%s, expected:%s ,wanted:%s" cid key (_s_ expected) (_s_ wanted) >>= fun () ->
	    Llio.output_int oc 0 >>= fun () ->
        Llio.output_string_option oc vo >>= fun () ->
        Lwt.return false
      end
    | USER_FUNCTION ->
      begin
	    Llio.input_string ic >>= fun name ->
	    Llio.input_string_option ic >>= fun po ->
	    Lwt.catch
	      (fun () ->
	        begin
	          backend # user_function name po >>= fun ro ->
              Lwt_log.debug_f ~section "[connection:%i] user_function, name:%s , po:%s" cid name (_s_ po) >>= fun () ->
	          Llio.output_int oc 0 >>= fun () ->
	          Llio.output_string_option oc ro >>= fun () ->
              Lwt.return false
	        end
	      )
	      (handle_exception cid oc)
      end
    | PREFIX_KEYS ->
      begin
	    Llio.input_bool   ic >>= fun allow_dirty ->
	    Llio.input_string ic >>= fun key ->
	    Llio.input_int    ic >>= fun max ->
	    backend # prefix_keys ~allow_dirty key max >>= fun keys ->
        Lwt_log.debug_f ~section "[connection:%i] PREFIX_KEYS, key:%s, max:%i" cid key max>>= fun () ->
        let size = List.length keys in
	    Llio.output_int oc 0 >>= fun () ->
        Lwt_log.debug_f ~section "size = %i" size >>= fun () ->
	    Llio.output_int oc size >>= fun () ->
	    Lwt_list.iter_s (Llio.output_string oc) keys >>= fun () ->
	    Lwt.return false
      end
    | MULTI_GET ->
      begin
	    Llio.input_bool ic >>= fun allow_dirty ->
	    Llio.input_int  ic >>= fun length ->
	    let rec loop keys i =
	      if i = 0
	      then Lwt.return keys
	      else
	        begin
	          Llio.input_string ic >>= fun key ->
	          loop (key :: keys) (i-1)
	        end
	    in
	    loop [] length >>= fun keys ->
	    Lwt.catch
	      (fun () ->
	        backend # multi_get ~allow_dirty keys >>= fun values ->
            Lwt_log.debug_f ~section "[connection:%i] MULTI-GET" cid >>= fun () ->
	        Llio.output_int oc 0 >>= fun () ->
	        Llio.output_int oc length >>= fun () ->
	        Lwt_list.iter_s (Llio.output_string oc) values >>= fun () ->
	        Lwt.return false
	      )
	      (handle_exception cid oc)
      end
    | SEQUENCE -> handle_sequence cid ~sync:false ic oc backend
    | SYNCED_SEQUENCE -> handle_sequence cid ~sync:true ic oc backend
    | MIGRATE_RANGE ->
      begin
        Lwt.catch(
          fun () ->
            Interval.input_interval ic >>= fun interval ->
            decode_sequence cid ic >>= fun updates ->
            let interval_update = Update.SetInterval interval in
            let updates' =  interval_update :: updates in
            backend # sequence ~sync:false updates' >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] MIGRATE_RANGE" cid >>= fun () ->
            response_ok_unit cid oc
        ) (handle_exception cid oc)
      end
    | STATISTICS ->
      begin
	    let s = backend # get_statistics () in
        Lwt_log.debug_f ~section "[connection:%i] STATISTICS" cid>>= fun () ->
	    let b = Buffer.create 100 in
	    Statistics.to_buffer b s;
	    let bs = Buffer.contents b in
	    Llio.output_int oc 0 >>= fun () ->
	    Llio.output_string oc bs >>= fun () ->
        Lwt.return false
      end
    | COLLAPSE_TLOGS ->
      begin
	    let sw () = Int64.bits_of_float (Unix.gettimeofday()) in
	    let t0 = sw() in
	    let cb' n =
	      Lwt_log.debug_f ~section "CB' %i" n >>= fun () ->
	      Llio.output_int oc 0 >>= fun () -> (* ok *)
	      Llio.output_int oc n >>= fun () ->
          Lwt_io.flush oc
	    in
	    let cb  =
          let count = ref 0 in
          fun () ->
	        Lwt_log.debug_f ~section "CB %i" !count >>= fun () ->
            let () = incr count in
	        let ts = sw() in
	        let d = Int64.sub ts t0 in
	        Llio.output_int oc 0 >>= fun () ->
	        Llio.output_int64 oc d >>= fun () ->
	        Lwt_io.flush oc
	    in
	    Llio.input_int ic >>= fun n ->
	    Lwt.catch
	      (fun () ->
	        Lwt_log.info_f "[connection: %i]... Start collapsing ... (n=%i)" cid n >>= fun () ->
	        backend # collapse n cb' cb >>= fun () ->
            Lwt_log.info_f "[connection:%i] ... Finished collapsing ..." cid >>= fun () ->
	        Lwt.return false 
	      )
	      (handle_exception cid oc)
      end
    | SET_INTERVAL ->
      begin
        Lwt.catch
	      (fun () ->
            Interval.input_interval ic >>= fun interval ->
            backend # set_interval interval >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] SET-INTERVAL" cid >>= fun () ->
            response_ok_unit cid oc
          )
          (handle_exception cid oc)
          
	  end
    | GET_INTERVAL ->
      begin
        Lwt.catch(
          fun() ->
            backend # get_interval () >>= fun interval ->
            Lwt_log.debug_f ~section "[connection:%i] GET-INTERVAL" cid >>= fun () ->
            Llio.output_int oc 0 >>= fun () ->
            Interval.output_interval oc interval >>= fun () ->
            Lwt.return false
        )
          (handle_exception cid oc)
      end
    | GET_ROUTING ->
      Lwt.catch
	    (fun () -> backend # get_routing () >>= fun routing ->
          Lwt_log.debug_f ~section "[connection:%i] GET-ROUTING" cid >>= fun () ->
	      Llio.output_int oc 0 >>= fun () ->
	      Routing.output_routing oc routing >>= fun () ->
	      Lwt.return false
	    )
	    (handle_exception cid oc)
    | SET_ROUTING ->
      begin
	    Routing.input_routing ic >>= fun routing ->
	    Lwt.catch
	      (fun () ->
	        backend # set_routing routing >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] SET-ROUTING" cid >>= fun () ->
	        response_ok_unit cid oc)
	      (handle_exception cid oc)
      end
    | SET_ROUTING_DELTA ->
      begin
        Lwt.catch(
          fun () ->
            Llio.input_string ic >>= fun left ->
            Llio.input_string ic >>= fun sep ->
            Llio.input_string ic >>= fun right ->
            backend # set_routing_delta left sep right >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] SET-ROUTING-DELTA, left:%s, set:%s, right:%s" cid left sep right >>= fun () ->
            response_ok_unit cid oc )
          (handle_exception cid oc)
      end
    | GET_KEY_COUNT ->
      begin
        Lwt.catch
          (fun() ->
            backend # get_key_count () >>= fun kc ->
            Lwt_log.debug_f ~section "[connection:%i] GET-KEY-COUNT" cid >>= fun () ->
            response_ok_int64 cid oc kc)
          (handle_exception cid oc)
      end
    | GET_DB ->
      begin
        Lwt.catch
          (fun() ->
            backend # get_db (Some oc) >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] GET-DB" cid >>= fun () ->
            Lwt.return false
          )
          (handle_exception cid oc)
      end
    | OPT_DB ->
      begin
        Lwt.catch
          ( fun () ->
            backend # optimize_db () >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] OPT-DB" cid >>= fun () ->
            response_ok_unit cid oc 
          )
          (handle_exception cid oc)
      end
    | DEFRAG_DB ->
      begin
        Lwt.catch
          (fun () -> backend # defrag_db () >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] DEFRAG-DB" cid >>= fun () ->
            response_ok_unit cid oc)
          (handle_exception cid oc)
      end
    | CONFIRM ->
	  begin
        Llio.input_string ic >>= fun key ->
        Llio.input_string ic >>= fun value ->
	    Lwt.catch
	      (fun () -> backend # confirm key value >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] CONFIRM, key:%s, value:%s" cid key value >>= fun () ->
	        response_ok_unit cid oc
	      )
	      (handle_exception cid oc)
	  end
        
    | GET_NURSERY_CFG ->
      begin
        Lwt.catch (
          fun () ->
            backend # get_routing () >>= fun routing ->
            backend # get_cluster_cfgs () >>= fun cfgs ->
            Lwt_log.debug_f ~section "[connection:%i] GET_NURSERY_CFG" cid >>= fun () ->
            let buf = Buffer.create 32 in
            NCFG.ncfg_to buf (routing,cfgs);
            Llio.output_int oc 0 >>= fun () ->
            Llio.output_string oc (Buffer.contents buf) >>= fun () ->
            Lwt.return false
        )
          ( handle_exception cid oc )
      end
    | SET_NURSERY_CFG ->
      begin
        Lwt.catch (
          fun () ->
            Llio.input_string ic >>= fun cluster_id ->
            ClientCfg.input_cfg ic >>= fun cfg ->
            backend # set_cluster_cfg cluster_id cfg >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] SET_NURSERY_CFG" cid >>= fun () ->
            response_ok_unit cid oc
        )
          ( handle_exception cid oc )
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
	        backend # get_fringe boundary direction >>= fun kvs ->
            Lwt_log.debug_f ~section "[connection:%i] GET_FRINGE backend op complete" cid >>= fun () ->
            Llio.output_int oc 0 >>= fun () ->
	        Llio.output_kv_list oc kvs >>= fun () ->
            Lwt_log.debug_f ~section "[connection:%i] GET_FRINGE all done" cid >>= fun () ->
	        Lwt.return false
	      )
	      (handle_exception cid oc)
      end
    | DELETE_PREFIX ->
      begin
        Lwt.catch 
          ( fun () ->
            Llio.input_string ic >>= fun prefix ->
            backend # delete_prefix prefix >>= fun n_deleted ->
            Lwt_log.debug_f ~section "[connection:%i] DELETE-PREFIX" cid >>= fun () ->
            Llio.output_int oc 0 >>= fun () ->
            Llio.output_int oc n_deleted >>= fun () ->
            Lwt.return false
          ) 
          (handle_exception cid oc)
      end
    | VERSION ->
      Llio.output_int oc 0 >>= fun () ->
      Llio.output_int oc Version.major >>= fun () ->
      Llio.output_int oc Version.minor >>= fun () ->
      Llio.output_int oc Version.patch >>= fun () ->
      let rest = Printf.sprintf "revision: %S\ncompiled: %S\nmachine: %S\n"
        Version.git_revision
        Version.compile_time
        Version.machine
      in
      Llio.output_string oc rest >>= fun () ->
      Lwt.return false
        
let protocol backend connection =
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
	  one_command connection backend >>= fun closed ->
	  Lwt_io.flush oc >>= fun() ->
	  if closed
	  then Lwt_log.debug_f ~section "[connection:%i] leaving client loop" cid
	  else loop ()
    end
  in
  prologue () >>= fun () ->
  loop ()
    
