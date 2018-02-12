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
open Lwt
open Node_cfg
open Remote_nodestream
open Tlogcollection
open Log_extra
open Tlogcommon

exception StoreAheadOfTlogs of (Int64.t * Sn.t)
exception StoreCounterTooLow of string

let _with_client_connection ~tls_ctx ~tcp_keepalive (ips,port) f =
  Client_helper.with_connection'
    ~tls:tls_ctx
    ~tcp_keepalive
    (List.map (fun ip -> Network.make_address ip port) ips)
    (fun _ -> f)


let head_saved_epilogue ~cluster_id hfn tlog_coll =
  (* maybe some tlogs must be thrown away because
     they were already collapsed into
     the received head
  *)
  Logger.info_f_ "head_saved_epilogue %s" hfn >>= fun () ->
  let module S = (val (Store.make_store_module (module Batched_store.Local_store))) in
  S.make_store
    ~lcnum:default_lcnum
    ~ncnum:default_ncnum ~read_only:true
    ~cluster_id
    hfn >>= fun store ->
  let hio = S.consensus_i store in
  S.close store ~flush:false ~sync:false >>= fun () ->
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

let catchup_tlog (type s) ~tls_ctx ~tcp_keepalive ~stop other_configs ~cluster_id  mr_name ((module S : Store.STORE with type t = s),store,tlog_coll)
  =
  tlog_coll # get_last_i () >>= fun current_i ->
  Logger.info_f_ "catchup_tlog %s from %s " (Sn.string_of current_i) mr_name >>= fun () ->
  let mr_addresses =
    try
      let mr_cfg =
      List.find (fun cfg -> Node_cfg.node_name cfg = mr_name)
                other_configs
    in
    Some (Node_cfg.client_addresses mr_cfg)
    with Not_found -> None
  in
  let head_saved_cb hfn =
    Logger.info_f_ "head_saved_cb %s" hfn >>= fun () ->
    head_saved_epilogue ~cluster_id hfn tlog_coll >>= fun () ->
    let when_closed () =
      Logger.debug_ "when_closed" >>= fun () ->
      let target_name = S.get_location store in
      File_system.copy_file hfn target_name ~overwrite:true ~throttling:0.0 >>= fun _copied ->
      Lwt.return ()
    in
    S.reopen store when_closed >>= fun () ->
    Lwt.return ()
  in

  let copy_tlog connection =
    make_remote_nodestream cluster_id connection >>= fun (client:nodestream) ->
    let f (i,value) =
      tlog_coll # log_value_explicit i value ~sync:false None >>= fun _ ->
      stop_fuse stop
    in
    client # iterate current_i
           f
           tlog_coll
           ~head_saved_cb
    >>= fun fs_changed ->
    if fs_changed then tlog_coll # invalidate();
    Lwt.return_unit
  in

  Lwt.catch
    (fun () ->
      match mr_addresses with
      | Some mr_addresses ->
         _with_client_connection  ~tls_ctx ~tcp_keepalive mr_addresses copy_tlog >>= fun () ->
         Logger.info_f_ "catchup_tlog completed" >>= fun () ->
         tlog_coll # get_last_i () >|= Option.some
      | None ->
         Logger.warning_f_
           "catchup tlog from unknown node:%S is config up to date?" mr_name
         >>= fun () ->
         (* return None when no catchup was done, so no entries are
          * applied for which there is possibly no consensus (yet or ever) *)
         Lwt.return_none
    )
    (fun exn ->
      Logger.warning_f_ "catchup_tlog failed: %S"
                        (Printexc.to_string exn) >>= fun () ->
      (* return None when no catchup was done, so no entries are
       * applied for which there is possibly no consensus (yet or ever) *)
      Lwt.return_none
    )


let prepare_and_insert_value (type s) (module S : Store.STORE with type t = s) me store i v =
  (* assume consensus for the masterset has only now been reached,
               this is playing it safe *)
  let v' = Value.fill_other_master_set me v in
  (* but do clear the master set if it's about the node itself,
               so the node can't erronously think it's master while it's not *)
  let v'' = Value.clear_self_master_set me v' in
  S.safe_insert_value store i v'' >>= fun _ ->
  Lwt.return ()

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
          prepare_and_insert_value (module S) me store pi pv >>= fun () ->
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
        prepare_and_insert_value (module S) me store i value
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
  let comp = Sn.compare start_i too_far_i in
  if comp > 0
  then
    let msg = Printf.sprintf
                "Store counter (%s) is ahead of tlog counter (%s). Aborting."
                (Sn.string_of start_i) (Sn.string_of too_far_i) in
    Logger.error_ msg >>= fun () ->
    Lwt.fail (StoreAheadOfTlogs(start_i, too_far_i))
  else if comp = 0
  then
    Logger.debug_f_ "catchup_store: start_i:%s == too_far_i:%s nothing to do"
                    (Sn.string_of start_i) (Sn.string_of too_far_i)
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
      let cb _ = Lwt.return_unit in
      tlog_coll # iterate start_i too_far_i
                f cb
      >>= fun () ->
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
            in
            Lwt.fail (StoreCounterTooLow msg)
          end
        else
          Lwt.return ()
      end
    end

let catchup
      ~tls_ctx ~tcp_keepalive
      ~stop me other_configs ~cluster_id ((_,_,tlog_coll) as dbt) mr_name =
  tlog_coll # get_last_i () >>= fun last_i ->
  Logger.info_f_ "CATCHUP start: I'm @ %s and %s is more recent"
    (Sn.string_of last_i) mr_name
  >>= fun () ->
  catchup_tlog
    ~tls_ctx ~tcp_keepalive
    ~stop other_configs ~cluster_id mr_name dbt >>= fun until_i ->
  match until_i with
  | None ->
     Logger.info_ "CATCHUP phase 1 failed, not going to the store"
  | Some until_i ->
     Logger.info_f_ "CATCHUP phase 1 done (until_i = %s); now the store"
                    (Sn.string_of until_i) >>= fun () ->
     catchup_store ~stop me dbt (Sn.succ until_i) >>= fun () ->
     Logger.info_ "CATCHUP end"

let verify_n_catchup_store
      (type s) ~stop me ?(apply_last_tlog_value=false)
      ((module S : Store.STORE with type t = s), store, tlog_coll)
  =
  tlog_coll # get_last_i () >>= fun current_i ->
  let too_far_i =
    if apply_last_tlog_value
    then
      Sn.succ current_i
    else
      current_i in
  let io_s = Log_extra.option2s Sn.string_of  in
  let si_o = S.consensus_i store in

  Logger.info_f_
    "verify_n_catchup_store; too_far_i=%s current_i=%s si_o:%s"
    (Sn.string_of too_far_i) (Sn.string_of current_i) (io_s si_o)
  >>= fun () ->

  match too_far_i, si_o with
  | i, None when i <= 0L -> Lwt.return ()
  | i, Some j when i = j -> Lwt.return ()
  | i, Some j when i > j -> catchup_store ~stop me ((module S),store,tlog_coll) too_far_i
  | _, None              -> catchup_store ~stop me ((module S),store,tlog_coll) too_far_i
  | _,_ ->
     let msg = Printf.sprintf
                 "too_far_i:%s, si_o:%s should not happen: tlogs have been removed?"
                 (Sn.string_of too_far_i) (io_s si_o)
     in
     Logger.fatal_ msg >>= fun () ->
     let maybe a = function | None -> a | Some b -> b in
     Lwt.fail (StoreAheadOfTlogs(maybe (-1L) si_o, too_far_i))

let last_entries
      (type s) (module S : Store.STORE with type t = s)
      store
      (tlog_collection:Tlogcollection.tlog_collection)
      (start_i:Sn.t) (oc:Lwt_io.output_channel)
  =
  Lwt.fail_with "no longer supported"

let last_entries2
      (type s) (module S : Store.STORE with type t = s)
      store
      (tlog_collection:Tlogcollection.tlog_collection)
      (start_i:Sn.t) (oc:Lwt_io.output_channel)
  =
  Logger.debug_f_ "last_entries2 %s" (Sn.string_of start_i) >>= fun () ->
  let consensus_i = S.consensus_i store in
  (* This version is kept for mixed setups were non-flexible nodes
     are catching up from flexible ones
   *)
  begin
    match consensus_i with
    | None -> Lwt.return ()
    | Some ci ->
       begin
         let stream_entries start_i too_far_i =
           Logger.debug_f_ "stream_entries :%s %s" (Sn.string_of start_i) (Sn.string_of too_far_i)
           >>= fun () ->
           let _write_entry entry =
             let i = Entry.i_of entry
             and v = Entry.v_of entry
             in
             Logger.debug_f_ "write_entry:%s" (Sn.string_of i) >>= fun () ->
             Tlogcommon.write_entry oc i v >>= fun _total_size ->
             Lwt.return_unit
           in
           let _cb _ = Lwt.return_unit in
           Llio.output_int oc 1 >>= fun () ->
           Lwt.catch
             (fun () -> tlog_collection # iterate start_i too_far_i _write_entry _cb)
             (function
              | Tlogcommon.TLogUnexpectedEndOfFile _ -> Lwt.return ()
              | ex -> Lwt.fail ex)
           >>= fun () ->
           Sn.output_sn oc (-1L)
         in
         let too_far_i = Sn.succ ci in
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
           stream_entries start_i2 too_far_i
         end
         >>= fun () ->
         Sn.output_sn oc (-2L) >>= fun () ->
         Logger.info_f_ "done with_last_entries2"
       end
  end

let last_entries3
      (type s) (module S : Store.STORE with type t = s)
      store
      (tlog_collection:Tlogcollection.tlog_collection)
      (start_i:Sn.t) (oc:Lwt_io.output_channel)
  =
  Logger.debug_f_ "last_entries3 %s" (Sn.string_of start_i) >>= fun () ->
  let consensus_i = S.consensus_i store in

  begin
    match consensus_i with
      | None -> Lwt.return ()
      | Some ci ->
         let stream_entries start_i too_far_i =
           Logger.debug_f_ "stream_entries :%s %s" (Sn.string_of start_i) (Sn.string_of too_far_i)
           >>= fun () ->
          let _write_entry entry =
            let i = Entry.i_of entry
            and v = Entry.v_of entry
            in
            Logger.debug_f_ "write_entry:%s" (Sn.string_of i) >>= fun () ->
            Tlogcommon.write_entry oc i v >>= fun _total_size ->
            Lwt.return_unit
          in
          let _cb _ = Lwt.return_unit in
          Llio.output_int oc 1 >>= fun () ->
          Lwt.catch
            (fun () -> tlog_collection # iterate start_i too_far_i
                                       _write_entry _cb
            )
            (function
              | Tlogcommon.TLogUnexpectedEndOfFile _ -> Lwt.return ()
              | ex -> Lwt.fail ex)
          >>= fun () ->
          Sn.output_sn oc (-1L)
        in
        let too_far_i = Sn.succ ci in
        let rec loop_files (start_i2:Sn.t) =
          Logger.debug_f_ "loop_files start_i2:%s" (start_i2 |> Sn.string_of) >>= fun () ->
          match tlog_collection # complete_file_to_deliver start_i2 with
          | None -> Lwt.return start_i2
          | Some (tlog_number, start_i2') ->
             begin
               Logger.debug_f_ "start_i2=%Li < %Li (outputting %03i) start_i2'=%Li"
                               start_i2 too_far_i tlog_number start_i2'
              >>= fun () ->
              Llio.output_int oc 3 >>= fun () ->
              tlog_collection # dump_tlog_file tlog_number oc >>= fun () ->
              loop_files start_i2'
            end
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
            if tlog_collection # is_rollover_point start_i2
            then
              Lwt.return start_i2
            else
              match tlog_collection # next_rollover start_i2 with
              | None -> Lwt.return start_i2
              | Some next_rollover ->
                 Logger.debug_f_ "next_rollover = %Li" next_rollover >>= fun () ->
                 (if next_rollover < too_far_i
                  then
                    begin
                      stream_entries start_i2 next_rollover >>= fun () ->
                      Lwt.return next_rollover
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
