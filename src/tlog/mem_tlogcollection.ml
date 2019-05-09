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

open Tlogcollection
open Tlogcommon

class mem_tlog_collection _name =
  object (self: #tlog_collection)

    val mutable data = []
    val mutable last_entry = (None: Entry.t option)

    method validate_last_tlog () =
      Lwt.return (TlogValidComplete, last_entry, None)

    method get_infimum_i () = Lwt.return Sn.start

    method get_last_i () =
      (match last_entry with
       | None -> Sn.start
       | Some entry -> Entry.i_of entry)
      |> Lwt.return

    method get_last_value i =
      (match last_entry with
       | None -> None
       | Some entry ->
          let i' = Entry.i_of entry in
          begin
            if i = i'
            then
              let v = Entry.v_of entry in
              Some v
            else
              None
          end)
      |> Lwt.return

    method get_last () =
      (match last_entry with
       | None -> None
       | Some e -> Some (Entry.v_of e, Entry.i_of e))
      |> Lwt.return


    method iterate from_i too_far_i f _cb =
      let data' =
        List.filter
          (fun entry ->
            let ei = Entry.i_of entry in
            ei >= from_i && ei < too_far_i)
          data in
      Lwt_list.iter_s f (List.rev data')

    method get_tlog_count() = failwith "get_tlog_count not supported"

    method dump_tlog_file _start_i _oc = failwith "dump_tlog_file not supported"

    method save_tlog_file _start_i _name _length _ic = failwith "save_tlog_file not supported"

    method which_tlog_file _start_i = failwith "which_tlog_file not supported"

    method tlogs_to_collapse ~head_i ~last_i ~tlogs_to_keep =
      let () = ignore (head_i,last_i,tlogs_to_keep) in
      failwith "tlogs_to_collapse not supported"

    method should_fsync _ = false
         
    method log_value i v = self #log_value_explicit i v ~sync_override:false None

    method accept i v = self # log_value i v
    method dump_head _oc = Llio.lwt_failfmt "dump_head not implemented"

    method save_head _ic = Llio.lwt_failfmt "save_head not implemented"

    method get_head_name () = failwith "get_head_name not implemented"

    method get_tlog_from_i _ = 0
    method get_start_i _n = Some Sn.zero
    method is_rollover_point _ = false
    method next_rollover _ = Some Int64.max_int

    method invalidate () = ()

    method log_value_explicit i (v:Value.t) ~sync_override marker =
      let () = ignore sync_override in
      let entry = Entry.make i v 0L marker in
      let () = data <- entry::data in
      let () = last_entry <- (Some entry) in
      Lwt.return 0


    method close ?(wait_for_compression = false) () =
        let () = ignore wait_for_compression in
        Lwt.return ()

    method remove_below _i = Lwt.return ()
    method complete_file_to_deliver _ = None
  end

let make_mem_tlog_collection
      ?cluster_id
      ?tlog_max_entries ?tlog_max_size _tlog_dir _tlf_dir _head_dir ~should_fsync name ~fsync_tlog_dir =
  let () = ignore (cluster_id, should_fsync,fsync_tlog_dir, tlog_max_entries, tlog_max_size) in
  let x = new mem_tlog_collection name in
  let x2 = (x :> tlog_collection) in
  Lwt.return x2
