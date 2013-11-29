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
open Node_cfg
open Remote_nodestream
open Tlogcollection
open Log_extra
open Update
open Store
open Tlogcommon

exception StoreAheadOfTlogs of (Int64.t * Sn.t)
exception StoreCounterTooLow of string

type store_tlc_cmp =
  | Store_n_behind
  | Store_1_behind
  | Store_tlc_equal
  | Store_ahead

let _with_client_connection (ips,port) f =
  let sl2s ss = list2s (fun s -> s) ss in
  let rec loop = function
    | [] -> Llio.lwt_failfmt "None of the ips: %s can be reached" (sl2s ips)
    | ip :: rest ->
      Lwt.catch
        (fun () ->
           let address = Network.make_address ip port in
           Lwt_io.with_connection address f)
        (fun exn ->
           Logger.info_f_ ~exn "ip = %s " ip >>= fun () ->
           match exn with
             | Unix.Unix_error(error,_,_) ->
               begin
                 match error with
                   | Unix.ECONNREFUSED
                   | Unix.ENETUNREACH
                   | Unix.EHOSTUNREACH
                   | Unix.EHOSTDOWN ->
                     loop rest
                   | _ -> Lwt.fail exn
               end
             | Lwt_unix.Timeout ->
               loop rest
             | _ -> Lwt.fail exn)
  in
  loop ips


let compare_store_tlc store tlc =
  tlc # get_last_i () >>= fun tlc_i ->
  let m_store_i = store # consensus_i () in
  match m_store_i with
    | None ->
      if tlc_i = ( Sn.succ Sn.start )
      then Lwt.return Store_1_behind
      else Lwt.return Store_n_behind
    | Some store_i when store_i = tlc_i -> Lwt.return Store_tlc_equal
    | Some store_i when store_i > tlc_i -> Lwt.return Store_ahead
    | Some store_i ->
      if store_i = (Sn.pred tlc_i)
      then Lwt.return Store_1_behind
      else Lwt.return Store_n_behind

let head_saved_epilogue hfn tlog_coll =
  (* maybe some tlogs must be thrown away because
     they were already collapsed into
     the received head
  *)
  Logger.info_f_ "head_saved_epilogue %s" hfn >>= fun () ->
  let module S = (val (Store.make_store_module (module Batched_store.Local_store))) in
  S.make_store ~lcnum:default_lcnum
    ~ncnum:default_ncnum ~read_only:true hfn >>= fun store ->
  let hio = S.consensus_i store in
  S.close store >>= fun () ->
  Logger.info_ "closed head" >>= fun () ->
  begin
    match hio with
      | None -> Lwt.return ()
      | Some head_i ->
        begin
	  Logger.info_f_ "head_i = %s" (Sn.string_of head_i) >>= fun () ->
          tlog_coll # remove_below head_i
        end
  end

let stop_fuse stop =
  if !stop
  then
    Lwt.fail Canceled
  else
    Lwt.return ()

let catchup_tlog (type s) ~stop me other_configs ~cluster_id  mr_name ((module S : Store.STORE with type t = s),store,tlog_coll)
  =
  let current_i = tlog_coll # get_last_i () in
  Logger.info_f_ "catchup_tlog %s" (Sn.string_of current_i) >>= fun () ->
  let mr_cfg = List.find (fun cfg -> Node_cfg.node_name cfg = mr_name)
                 other_configs in
  let mr_addresses = Node_cfg.client_addresses mr_cfg
  and mr_name = Node_cfg.node_name mr_cfg in
  Logger.info_f_ "getting last_entries from %s" mr_name >>= fun () ->
  let head_saved_cb hfn =
    Logger.info_f_ "head_saved_cb %s" hfn >>= fun () ->
    head_saved_epilogue hfn tlog_coll >>= fun () ->
    let when_closed () =
      Logger.debug_ "when_closed" >>= fun () ->
      let target_name = S.get_location store in
      File_system.copy_file hfn target_name true
    in
    S.reopen store when_closed >>= fun () ->
    Lwt.return ()
  in

  let copy_tlog connection =
    make_remote_nodestream cluster_id connection >>= fun (client:nodestream) ->
    let f (i,value) =
      tlog_coll # log_value_explicit i value false None >>= fun _ ->
      stop_fuse stop
    in

    client # iterate current_i f tlog_coll ~head_saved_cb
  in

  Lwt.catch
    (fun () ->
       _with_client_connection mr_addresses copy_tlog >>= fun () ->
      Logger.info_f_ "catchup_tlog completed"
    )
    (fun exn -> Logger.warning_ ~exn "catchup_tlog failed")
  >>= fun () ->
  Lwt.return (tlog_coll # get_last_i ())

let prepare_value pv me =
  (* assume consensus for the masterset has only now been reached,
     this is playing it safe *)
  let pv' = Value.fill_if_master_set pv in
  (* but do clear the master set if it's about the node itself,
     so the node can't erronously think it's master while it's not *)
  let pv'' = Value.clear_self_master_set me pv' in
  pv''

let make_f ~stop (type s) (module S : Store.STORE with type t = s) me log_i acc store entry =
  stop_fuse stop >>= fun () ->
  let i = Entry.i_of entry
  and value = Entry.v_of entry
  in
  match !acc with
    | None ->
      let () = acc := Some(i,value) in
      Logger.debug_f_ "value %s has no previous" (Sn.string_of i) >>= fun () ->
      Lwt.return ()
    | Some (pi,pv) ->
      if pi < i
      then
        begin
          log_i pi >>= fun () ->
          let pv' = prepare_value pv me in
          S.safe_insert_value store pi pv' >>= fun _ ->
          let () = acc := Some(i,value) in
          Lwt.return ()
        end
      else
        begin
          Logger.debug_f_ "%s => skip" (Sn.string_of pi) >>= fun () ->
          let () = acc := Some(i,value) in
          Lwt.return ()
        end


let epilogue (type s) (module S : Store.STORE with type t = s) me acc store =
  match !acc with
    | None -> Lwt.return ()
    | Some(i,value) ->
      begin
        Logger.debug_f_ "%s => store" (Sn.string_of i) >>= fun () ->
        let pv' = prepare_value value me in
        S.safe_insert_value store i pv'  >>= fun _ -> Lwt.return ()
      end


let catchup_store ~stop (type s) me
      ((module S : Store.STORE with type t = s), store,
       (tlog_coll: tlog_collection))
      (too_far_i:Sn.t) =
  Logger.info_ "replaying log to store"
  >>= fun () ->
  let store_i = S.consensus_i store in
  let start_i =
    match store_i with
      | None -> Sn.start
      | Some i -> Sn.succ i
  in
  if Sn.compare start_i too_far_i > 0
  then
    let msg = Printf.sprintf
                "Store counter (%s) is ahead of tlog counter (%s). Aborting."
                (Sn.string_of start_i) (Sn.string_of too_far_i) in
    Logger.error_ msg >>= fun () ->
    Lwt.fail (StoreAheadOfTlogs(start_i, too_far_i))
  else
    begin
      Logger.debug_f_ "will replay starting from %s into store, too_far_i:%s"
        (Sn.string_of start_i) (Sn.string_of too_far_i)
      >>= fun () ->
      let acc = ref None in
      let maybe_log_progress pi =
        let (+:) = Sn.add
        and border = Sn.of_int 100 in
        if pi < start_i +: border ||
           pi +: border > too_far_i
        then
          Logger.debug_f_ "%s => store" (Sn.string_of pi)
        else
        if Sn.rem pi border = Sn.zero
        then
          Logger.debug_f_ "... %s ..." (Sn.string_of pi)
        else
          Lwt.return ()
      in
      let f = make_f ~stop (module S) me maybe_log_progress acc store in
      tlog_coll # iterate start_i too_far_i f >>= fun () ->
      epilogue (module S) me acc store >>= fun () ->
      let store_i' = S.consensus_i store in
      Logger.info_f_ "catchup_store completed, store is @ %s"
        ( option2s Sn.string_of store_i')
      >>= fun () ->
      begin
        let si = match store_i' with
          | Some i -> i
          | None -> Sn.start
        in
        let pred_too_far_i = Sn.pred too_far_i in
        if si < pred_too_far_i
        then
          begin
            let msg =
              Printf.sprintf
                "Catchup store failed. Store counter is too low: %s < %s"
                (Sn.string_of si) (Sn.string_of pred_too_far_i)
            and n_entries = Sn.of_int !Tlogcommon.tlogEntriesPerFile in
            if Sn.rem si n_entries = Sn.pred n_entries
            then
              let ssi = Sn.succ si in
              tlog_coll # which_tlog_file ssi >>= function
              | None ->
                let n = tlog_coll # get_tlog_from_i ssi in
                let ni = Sn.to_int n in
                Lwt.return
                  (Printf.sprintf
                     "%s (found tlog nor archive for %3i)" msg ni)
              | Some _ -> Lwt.return msg
            else
              Lwt.return msg
          end
          >>= fun msg -> Lwt.fail (StoreCounterTooLow msg)
        else
          Lwt.return ()
      end >>= fun () ->
      S.flush store
    end

let catchup ~stop me other_configs ~cluster_id ((_,_,tlog_coll) as dbt) mr_name =
  Logger.info_f_ "CATCHUP start: I'm @ %s and %s is more recent"
    (Sn.string_of (tlog_coll # get_last_i ())) mr_name
  >>= fun () ->
  catchup_tlog ~stop me other_configs ~cluster_id mr_name dbt >>= fun until_i ->
  Logger.info_f_ "CATCHUP phase 1 done (until_i = %s); now the store"
    (Sn.string_of until_i) >>= fun () ->
  catchup_store ~stop me dbt (Sn.succ until_i) >>= fun () ->
  Logger.info_ "CATCHUP end"

let verify_n_catchup_store (type s) ~stop me ?(apply_last_tlog_value=false) ((module S : Store.STORE with type t = s), store, tlog_coll) =
  let current_i = tlog_coll # get_last_i () in
  let too_far_i =
    if apply_last_tlog_value
    then
      Sn.succ current_i
    else
      current_i in
  let io_s = Log_extra.option2s Sn.string_of  in
  let si_o = S.consensus_i store in
  Logger.info_f_ "verify_n_catchup_store; too_far_i=%s current_i=%s si_o:%s"
    (Sn.string_of too_far_i) (Sn.string_of current_i) (io_s si_o) >>= fun () ->
   match too_far_i, si_o with
    | i, None when i <= 0L -> Lwt.return ()
    | i, Some j when i = j -> Lwt.return ()
    | i, Some j when i > j -> 
      catchup_store ~stop me ((module S),store,tlog_coll) too_far_i
    | i, None ->
      catchup_store ~stop me ((module S),store,tlog_coll) too_far_i
    | _,_ ->
      let msg = Printf.sprintf
        "too_far_i:%s, si_o:%s should not happen: tlogs have been removed?"
        (Sn.string_of too_far_i) (io_s si_o)
      in
      Logger.fatal_ msg >>= fun () ->
      let maybe a = function | None -> a | Some b -> b in
      Lwt.fail (StoreAheadOfTlogs(maybe (-1L) si_o, too_far_i))

let get_db db_name cluster_id cfgs =

  let get_db_from_stream conn =
    make_remote_nodestream cluster_id conn >>= fun (client:nodestream) ->
    client # get_db db_name
  in
  let try_db_download m_success cfg =
    begin
      match m_success with
        | Some s -> Lwt.return m_success
        | None ->
          Lwt.catch
            ( fun () ->
               Logger.info_f_ "get_db: Attempting download from %s" (Node_cfg.node_name cfg) >>= fun () ->
               let mr_addresses = Node_cfg.client_addresses cfg in
               _with_client_connection mr_addresses get_db_from_stream >>= fun () ->
               Logger.info_ "get_db: Download succeeded" >>= fun () ->
               Lwt.return (Some (Node_cfg.node_name cfg))
            )
            ( fun e ->
               Logger.error_f_ "get_db: DB download from %s failed (%s)" (Node_cfg.node_name cfg) (Printexc.to_string e) >>= fun () ->
               Lwt.return None
            )
    end
  in
  Lwt_list.fold_left_s try_db_download None cfgs




let last_entries
      (type s) (module S : Store.STORE with type t = s)
      store tlog_collection (start_i:Sn.t) (oc:Lwt_io.output_channel)
  =
  (* This one is kept (for how long?)
     for x-version clusters during upgrades
  *)
  Logger.warning_f_ "DEPRECATED : last_entries " >>= fun () ->
  Logger.debug_f_ "last_entries %s" (Sn.string_of start_i) >>= fun () ->
  let consensus_i = S.consensus_i store in
  begin
    match consensus_i with
      | None -> Lwt.return ()
      | Some ci ->
        begin
          tlog_collection # get_infimum_i () >>= fun inf_i ->
          let too_far_i = Sn.succ ci in
          Logger.debug_f_
            "inf_i:%s too_far_i:%s" (Sn.string_of inf_i)
            (Sn.string_of too_far_i)
          >>= fun () ->
          begin
            if start_i < inf_i
            then
              begin
                Llio.output_int oc 2 >>= fun () ->
                tlog_collection # dump_head oc
              end
            else
              Lwt.return start_i
          end
          >>= fun start_i2->


          let step = Sn.of_int (!Tlogcommon.tlogEntriesPerFile) in
          let rec loop_parts (start_i2:Sn.t) =
            if Sn.rem start_i2 step = Sn.start &&
               Sn.add start_i2 step < too_far_i
            then
              begin
                Logger.debug_f_ "start_i2=%Li < %Li" start_i2 too_far_i
                >>= fun () ->
                Llio.output_int oc 3 >>= fun () ->
                tlog_collection # dump_tlog_file start_i2 oc
                >>= fun start_i2' ->
                loop_parts start_i2'
              end
            else
              Lwt.return start_i2
          in
          loop_parts start_i2
          >>= fun start_i3 ->
          Llio.output_int oc 1 >>= fun () ->
          let f entry =
            let i = Entry.i_of entry
            and v = Entry.v_of entry
            in
            Tlogcommon.write_entry oc i v
          in
          Lwt.catch
            (fun () -> tlog_collection # iterate start_i3 too_far_i f)
            (function
              | Tlogcommon.TLogUnexpectedEndOfFile _ -> Lwt.return ()
              | ex -> Lwt.fail ex) >>= fun () ->
          Sn.output_sn oc (-1L)
        end
  end
  >>= fun () ->
  Logger.info_f_ "done with_last_entries"


let last_entries2
      (type s) (module S : Store.STORE with type t = s)
      store tlog_collection (start_i:Sn.t) (oc:Lwt_io.output_channel)
  =
  Logger.debug_f_ "last_entries2 %s" (Sn.string_of start_i) >>= fun () ->
  let consensus_i = S.consensus_i store in

  begin
    match consensus_i with
      | None -> Lwt.return ()
      | Some ci ->

        let step = Sn.of_int (!Tlogcommon.tlogEntriesPerFile) in

        let stream_entries start_i too_far_i =
          let _write_entry entry =
            let i = Entry.i_of entry
            and v = Entry.v_of entry
            in
            Tlogcommon.write_entry oc i v
          in
          Llio.output_int oc 1 >>= fun () ->
          Lwt.catch
            (fun () -> tlog_collection # iterate start_i too_far_i _write_entry)
            (function
              | Tlogcommon.TLogUnexpectedEndOfFile _ -> Lwt.return ()
              | ex -> Lwt.fail ex)
          >>= fun () ->
          Sn.output_sn oc (-1L)
        in
        let too_far_i = Sn.succ ci in
        let rec loop_files (start_i2:Sn.t) =
          if Sn.rem start_i2 step = Sn.start &&
             Sn.add start_i2 step < too_far_i
          then
            begin
              Logger.debug_f_ "start_i2=%Li < %Li" start_i2 too_far_i
              >>= fun () ->
              Llio.output_int oc 3 >>= fun () ->
              tlog_collection # dump_tlog_file start_i2 oc
              >>= fun start_i2' ->
              loop_files start_i2'
            end
          else
            Lwt.return start_i2
        in
        let maybe_dump_head () =
          tlog_collection # get_infimum_i () >>= fun inf_i ->
          Logger.debug_f_
            "inf_i:%s too_far_i:%s" (Sn.string_of inf_i)
            (Sn.string_of too_far_i)
          >>= fun () ->
          begin
            if start_i < inf_i
            then
              begin
                Llio.output_int oc 2 >>= fun () ->
                tlog_collection # dump_head oc
              end
            else
              Lwt.return start_i
          end
        in
        begin
          maybe_dump_head () >>= fun start_i2->

          (* maybe stream a bit *)
          begin
            let (-:) = Sn.sub
            and (+:) = Sn.add
            and (%:) = Sn.rem
            in
            let next_rotation = start_i2 +: (step -: (start_i2 %: step)) in
            Logger.debug_f_ "next_rotation = %Li" next_rotation >>= fun () ->
            (if next_rotation < too_far_i
             then
               begin
                 stream_entries start_i2 next_rotation >>= fun () ->
                 Lwt.return next_rotation
               end
             else Lwt.return start_i2
            )
          end
          >>= fun start_i3 ->
          (* push out files *)
          loop_files start_i3
          >>= fun start_i4 ->
          (* epilogue: *)
          stream_entries start_i4 too_far_i
        end
  end
  >>= fun () ->
  Sn.output_sn oc (-2L) >>= fun () ->
  Logger.info_f_ "done with_last_entries"
