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
type 'a store_maker= ?read_only:bool -> string -> 'a Lwt.t
open Tlogcommon

let section = Logger.Section.main

let collapse_until (type s) (tlog_coll:Tlogcollection.tlog_collection)
    (module S : Store.STORE with type t = s)
    ((copy_store : string -> string ->
                   overwrite:bool -> throttling:float -> unit Lwt.t),
     (head_location:string),
     (throttling:float)
    )
    (too_far_i:Sn.t)
    (cb: Sn.t -> unit Lwt.t)
    (slowdown : float option)=

  let new_location = head_location ^ ".clone" in
  Logger.info_f_ "Creating db clone at %s (throttling:%f)"
                 new_location throttling
  >>= fun () ->
  Lwt.catch (
    fun () ->
      copy_store head_location new_location
                 ~overwrite:true
                 ~throttling
  ) (
    function
      | Not_found -> Logger.debug_f_ "head db at '%s' does not exist" head_location
      | e -> fail e
  )
  >>= fun () ->
  Logger.debug_f_ "Creating store at %s" new_location >>= fun () ->
  S.make_store ~lcnum:Node_cfg.default_lcnum
    ~ncnum:Node_cfg.default_ncnum
    new_location >>= fun new_store ->
  Lwt.finalize (
    fun () ->
      tlog_coll # get_infimum_i () >>= fun min_i ->
      let first_tlog = (Sn.to_int min_i) /  !Tlogcommon.tlogEntriesPerFile in
      let store_i = S.consensus_i new_store in
      let tfs = Sn.string_of too_far_i in
      let tlog_entries_per_file = Sn.of_int (!Tlogcommon.tlogEntriesPerFile) in
      let processed = ref 0 in
      let start_i =
        begin
          match store_i with
            | None -> Sn.start
            | Some i -> Sn.succ i
        end
      in

      begin
        if start_i >= too_far_i
        then
          let msg = Printf.sprintf
                      "Store counter (%s) is ahead of end point (%s). Aborting"
                      (Sn.string_of start_i) tfs in
          Lwt.fail (Failure msg)
        else
          let acc = ref None in
          let maybe_log =
            begin
              let lo = Sn.add start_i   (Sn.of_int 10) in
              let hi = Sn.sub too_far_i (Sn.of_int 10) in
              function
                | b when b < lo || b > hi ->  Logger.debug_f_ "%s => store" (Sn.string_of b)
                | b when b = lo -> Logger.debug_ " ... => store"
                | _ -> Lwt.return ()
            end
          in
          let slowdown =
            match slowdown with
            | None ->
               fun f ->
               f ()
            | Some factor ->
               fun f ->
               let t0 = Unix.gettimeofday () in
               f () >>= fun r ->
               let t1 = Unix.gettimeofday () in
               let period = ((t1 -. t0) *. factor) in
               Logger.debug_f_ "Sleeping for %f to slow down replay to store" period >>= fun () ->
               Lwt_unix.sleep period >>= fun () ->
               Lwt.return r
          in
          let add_to_store entry =
            let i = Entry.i_of entry
            and value = Entry.v_of entry
            in
            begin
              match !acc with
                | None ->
                  begin
                    let () = acc := Some(i,value) in
                    Logger.debug_f_ "update %s has no previous" (Sn.string_of i)
                    >>= fun () ->
                    Lwt.return ()
                  end
                | Some (pi,pv) ->
                  if pi < i then
                    begin
                      maybe_log pi >>= fun () ->
                      begin
                        if Sn.rem pi tlog_entries_per_file = 0L
                        then

                          cb (Sn.of_int (first_tlog + !processed)) >>= fun () ->
                          Lwt.return( processed := !processed + 1 )
                        else
                          Lwt.return ()
                      end
                      >>= fun () ->
                      slowdown (fun () -> S.safe_insert_value new_store pi pv) >>= fun _ ->
                      let () = acc := Some(i,value) in
                      Lwt.return ()
                    end
                  else
                    begin
                      Logger.debug_f_ "%s => skip" (Sn.string_of pi) >>= fun () ->
                      let () = acc := Some(i,value) in
                      Lwt.return ()
                    end
            end
          in
          Logger.debug_f_ "collapse_until: start_i=%s" (Sn.string_of start_i)
          >>= fun () ->
          tlog_coll # iterate start_i too_far_i add_to_store
          >>= fun () ->
          let m_si = S.consensus_i new_store in
          let si =
            begin
              match m_si with
                | None -> Sn.start
                | Some i -> i
            end
          in

          Logger.debug_f_ "Done replaying to head (%s : %s)" (Sn.string_of si) (Sn.string_of too_far_i) >>= fun() ->
          begin
            if si = Sn.pred (Sn.pred too_far_i) then
              Lwt.return ()
            else
              let msg = Printf.sprintf "Head db has invalid counter: %s" (Sn.string_of si) in
              Logger.debug_ msg >>= fun () ->
              S.close new_store >>= fun () ->
              Lwt.fail (Failure msg)
          end

      end
      >>= fun () ->
      Logger.debug_f_ "Relocating store to %s" head_location >>= fun () ->
      S.close new_store >>= fun () ->
      File_system.rename new_location head_location
  )
    (
      fun () ->
      S.close new_store >>= fun ()->
      Lwt.catch
      (fun () -> File_system.exists new_location >>=
                   function
                   | true ->
                      begin
                        Logger.info_f_ "unlinking %s" new_location >>= fun ()->
                        File_system.unlink new_location
                      end
                   | false -> Lwt.return ())
      (fun exn -> Logger.warning_f_ ~exn "ignoring failure in cleanup")
    )

let _head_i (type s) (module S : Store.STORE with type t = s) head_location =
  Lwt.catch
    (fun () ->
       let read_only=true in
       S.make_store
         ~lcnum:Node_cfg.default_lcnum
         ~ncnum:Node_cfg.default_ncnum
         ~read_only head_location >>= fun head ->
       let head_io = S.consensus_i head in
       S.close head ~sync:false ~flush:false >>= fun () ->
       Lwt.return head_io
    )
    (fun exn ->
       Logger.info_f_ ~exn "returning assuming no I %S" head_location >>= fun () ->
       Lwt.return None
    )

let collapse_many
      (type s) tlog_coll
      (module S : Store.STORE with type t = s)
      (store_fs: 'b * string * float)
      tlogs_to_keep cb' cb slowdown =

  Logger.debug_f_ "collapse_many" >>= fun () ->
  tlog_coll # get_tlog_count () >>= fun total_tlogs ->
  Logger.debug_f_ "total_tlogs = %i; tlogs_to_keep=%i" total_tlogs tlogs_to_keep >>= fun () ->
  let (_,(head_location:string), _) = store_fs in
  _head_i (module S) head_location >>= fun head_io ->
  let last_i = tlog_coll # get_last_i () in
  Logger.debug_f_ "head @ %s : last_i %s " (Log_extra.option2s Sn.string_of head_io) (Sn.string_of last_i)
  >>= fun () ->
  let head_i = match head_io with None -> Sn.start | Some i -> i in
  let lag = Sn.to_int (Sn.sub last_i head_i) in
  let npt = !Tlogcommon.tlogEntriesPerFile in
  let tlog_lag = (lag +npt - 1)/ npt in
  let tlogs_to_collapse = tlog_lag - tlogs_to_keep - 1 in
  Logger.debug_f_ "tlog_lag = %i; tlogs_to_collapse = %i" tlog_lag tlogs_to_collapse >>= fun () ->
  if tlogs_to_collapse <= 0
  then
    Logger.info_f_ "Nothing to collapse..." >>= fun () ->
    cb' 0
  else
    begin
      Logger.info_f_ "Going to collapse %d tlogs" tlogs_to_collapse >>= fun () ->
      cb' (tlogs_to_collapse+1) >>= fun () ->
      let g_too_far_i = Sn.add (Sn.of_int 2) (Sn.add head_i (Sn.of_int (tlogs_to_collapse * npt))) in
      (* +2 because before X goes to the store, you need to have seen X+1 and thus too_far = X+2 *)
      Logger.debug_f_ "g_too_far_i = %s" (Sn.string_of g_too_far_i) >>= fun () ->
      collapse_until tlog_coll (module S) store_fs g_too_far_i cb slowdown >>= fun () ->
      tlog_coll # remove_oldest_tlogs tlogs_to_collapse >>= fun () ->
      cb (Sn.div g_too_far_i (Sn.of_int !Tlogcommon.tlogEntriesPerFile))
    end
