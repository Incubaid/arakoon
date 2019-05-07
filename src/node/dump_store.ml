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
open Routing
open Node_cfg.Node_cfg
open Arakoon_interval
open Tlog_map

module S = (val (Store.make_store_module (module Batched_store.Local_store)))

let try_fetch name (f:unit -> 'a) (r2s: 'a -> string)  =
  Lwt.catch
    (fun () ->
       let r = f () in
       let s = r2s r in
       Lwt_io.printlf "%s: %s" name s
    )
    (function
      | Not_found -> Lwt_io.printlf "%s : --" name
      | e ->         Lwt_io.printlf "%s : %s" name (Printexc.to_string e)
    )


let _dump_routing store =  try_fetch "routing" (fun () -> S.get_routing store) Routing.to_s

let _dump_interval store = try_fetch "interval" (fun () -> S.get_interval store) Interval.to_string

let _dump_cluster_id store = try_fetch "cluster_id" (fun () -> S.get_cluster_id store) Log_extra.string_option2s

let summary store =
  let consensus_i = S.consensus_i store
  and mdo = S.who_master store
  in
  Lwt_io.printlf "i: %s" (Log_extra.option2s Sn.string_of consensus_i) >>= fun () ->
  let s =
    match mdo with
      | None -> "None"
      | Some (m,e) -> Printf.sprintf "Some(%s,%f)" m e
  in
  Lwt_io.printlf "master: %s" s
  >>= fun () ->
  _dump_routing store >>= fun () ->
  _dump_interval store >>= fun () ->
  _dump_cluster_id store

let dump_store ~dump_values filename =
  let () = ignore dump_values in
  let t () =
    S.make_store ~lcnum:1024 ~ncnum:512 ~read_only:true filename >>= fun store ->
    summary store >>= fun () ->
    let rec loop (first:string option) =
      if first = None then Lwt.return_unit
      else
        let _n,kvs = S.range_entries store first false None true 100 in
        Lwt_list.fold_left_s
          (fun _last (k,v)  ->
            let (kx:string) = Key.get k in
            Lwt_io.printlf "%S\t%S" kx v >>= fun () ->
            Lwt.return (Some kx)
          )
          None kvs
        >>= fun lo ->
        loop lo
    in
    loop (Some "")
    >>= fun () ->
    S.close store ~sync:false ~flush:false
  in
  Lwt_extra.run t;
  0

exception ExitWithCode of int;;

let inject_as_head fn node_id cfg_url ~force ~in_place =

  let t () =
    retrieve_cfg cfg_url >>= fun cluster_cfg ->
    let node_cfgs = List.filter
                      (fun ncfg -> node_name ncfg = node_id)
                      cluster_cfg.cfgs
    in
    let node_cfg = match node_cfgs with
      | [] -> failwith (Printf.sprintf "unknown node: %S" node_id)
      | x :: _ -> x
    in
    TlogMap.make node_cfg.tlog_dir node_cfg.tlx_dir node_id ~check_marker:false
    >>= fun (tlog_map,_,_) ->
    let head_dir = node_cfg.head_dir in
    let old_head_name = Filename.concat head_dir Tlc2.head_fname  in

    let read_i head_name =
      S.make_store ~lcnum:1024 ~ncnum:512 head_name >>= fun head ->
      let head_i = S.consensus_i head in
      S.close ~flush:false ~sync:false head >>= fun () ->
      Lwt.return head_i in

    (Lwt.catch
       (fun () -> read_i old_head_name)
       (fun exn ->
          if force
          then
            Lwt.return None
          else
            begin
              Lwt_io.printl (Printexc.to_string exn) >>= fun () ->
              Lwt.fail (ExitWithCode 3)
            end)) >>= fun old_head_i ->
    (Lwt.catch
       (fun () -> read_i fn)
       (fun exn ->
          Lwt_io.printl (Printexc.to_string exn) >>= fun () ->
          Lwt.fail (ExitWithCode 4))) >>= fun new_head_i ->

    Lwt_io.printlf "# %s @ %s" old_head_name (Log_extra.option2s Sn.string_of old_head_i) >>= fun () ->
    Lwt_io.printlf "# %s @ %s" fn (Log_extra.option2s Sn.string_of new_head_i) >>= fun () ->


    let ok = match old_head_i, new_head_i with
      | None,None -> false
      | Some _, None -> false
      | None, Some _ -> true
      | Some oi, Some ni -> ni > oi
    in
    Lwt_io.printlf "# head is newer">>= fun () ->
    if not ok then failwith "new head is not an improvement";
    let bottom_n = match new_head_i with
      | None -> failwith "can't happen"
      | Some i -> TlogMap.outer_of_i tlog_map i
    in
    begin
      if (not in_place)
      then begin
        Lwt_io.printf "cp %S %S" fn old_head_name >>=fun () ->
        File_system.copy_file fn old_head_name ~overwrite:true ~throttling:0.0 >>= fun _ ->
        Lwt.return ()
      end
      else begin
        Lwt_io.printf "rename %S %S" fn old_head_name >>= fun () ->
        File_system.safe_rename fn old_head_name
      end
    end
    >>= fun () ->
    Lwt_io.printlf "# [OK]">>= fun () ->
    Lwt_io.printlf "# remove superfluous .tlx files" >>= fun () ->
    TlogMap.get_tlog_names tlog_map >>= fun tlns ->
    let old_tlns = List.filter (fun tln -> let n = Tlog_map.get_number tln in n < bottom_n) tlns in
    Lwt_list.iter_s
      (fun old_tln ->
         let canonical = TlogMap.get_full_path tlog_map old_tln in
         Lwt_io.printlf "rm %s" canonical >>= fun () ->
         File_system.unlink canonical
      ) old_tlns >>= fun () ->
    Lwt_io.printlf "# [OK]" >>= fun () ->
    Lwt.return 0

  in
  try
    Lwt_extra.run t
  with
    | ExitWithCode i -> i
    | exn -> raise exn


let verify_store fn start_key =
  let open Camltc in
  let open Local_store in
  let read_only = true in
  let t () =
    Local_store.make_store ~lcnum:1024 ~ncnum:512 read_only fn >>= fun ls ->
    let go () =
      Bdb.with_cursor
        (Hotc.get_bdb ls.db)
        (fun bdb cursor ->
          let () = Bdb.first bdb cursor in
          let () = match start_key with
            | None -> ()
            | Some key -> Bdb.jump bdb cursor key
          in
          let rec loop checksum cont i =
            if not cont
            then
              Lwt_io.eprintlf "     ...\n%10i: checksum =  %04lx" i checksum >>= fun () ->
              Lwt.return 0
            else
              begin
                let key = Bdb.key bdb cursor in
                let value = Bdb.value bdb cursor in
                let checksum' =
                  let tmp = Crc32c.update_crc32c checksum key    0 (String.length key)   in
                  let tmp = Crc32c.update_crc32c tmp      value  0 (String.length value) in
                  tmp
                in
                begin
                  if (i < 10 || i mod 10000 = 0)
                  then Lwt_io.eprintlf "%10i: %S\tok%!" i key
                  else Lwt.return_unit
                end
                >>= fun () ->
                let cont' = try Bdb.next bdb cursor; true with Not_found -> false in
                loop checksum' cont' (i+1)
              end;
          in
          let checksum0 = 0xffffl in
          loop checksum0 true 0
        )
    in
    go ()
  in
  Lwt_extra.run t

let inspect_store fn left max_results =
  let t () =
    let open Camltc in
    let open Local_store in
    make_store ~lcnum:1024 ~ncnum:512 true fn >>= fun ls ->
    Bdb.with_cursor
      (Hotc.get_bdb ls.db)
      (fun bdb cursor ->
        let () = match left with
          | None -> Bdb.first bdb cursor
          | Some left -> Bdb.jump bdb cursor left
        in
        let rec loop i =
          if i < max_results
          then
            begin
              let key = Bdb.key bdb cursor in
              let value = Bdb.value bdb cursor in
              Lwt_io.printlf "%i: key=%S value=%S" i key value |> Lwt.ignore_result;
              let continue =
                try Bdb.next bdb cursor; true
                with Not_found -> false
              in
              if continue
              then loop (i + 1)
              else Lwt_io.printlf "Finished iterating over db" |> Lwt.ignore_result
            end
        in
        loop 0
      );
    Lwt.return 0
  in
  Lwt_extra.run
    (fun () ->
      Lwt.catch
        t
        (fun exn ->
          Lwt_io.printlf "Got exception during inspect store: %s" (Printexc.to_string exn) >>= fun () ->
          Lwt.return 1
    ))

let set_store_i fn new_i =
  let t () =
    Local_store.make_store ~lcnum:1024 ~ncnum:512 false fn >>= fun store ->
    let current_i =
      try
        let s = Local_store.get store Simple_store.__i_key in
        Some (Sn.sn_from (Llio.make_buffer s 0))
      with Not_found ->
        None
    in
    Lwt_io.printlf
      "Current store i:%s, replacing with %i"
      (Log_extra.option2s Int64.to_string current_i)
      new_i >>= fun () ->

    let buf = Buffer.create 8 in
    Sn.sn_to buf (Int64.of_int new_i);
    Local_store.with_transaction
      store
      (fun tx ->
        Local_store.set store tx Simple_store.__i_key (Buffer.contents buf);
        Lwt.return ()) >>= fun () ->

    Lwt.return 0
  in
  Lwt_extra.run t

let store_set fn key value =
  let t () =
    Local_store.make_store ~lcnum:1024 ~ncnum:512 false fn >>= fun store ->
    Local_store.with_transaction
      store
      (fun tx ->
        Local_store.set store tx key value;
        Lwt.return_unit)
    >>= fun () ->
    Lwt.return 0
  in
  Lwt_extra.run t
