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
open Arakoon_client

let _cat s i = s ^ (Printf.sprintf "%08i" i)

let _progress t0 n step (oc:Lwt_io.output_channel) =
  if n > 0 && n mod step = 0 then
    begin
      let ti = Unix.gettimeofday() in
      let delta = ti -. t0 in
      let f = if n mod (step * 5) = 0
        then Lwt_io.fprintlf oc
        else Lwt_io.fprintf oc
      in
      f "%9i (% 4.0fs)%!" n delta
    end
  else
    Lwt.return()

let check_timing f f_msg =
  let t0 = Unix.gettimeofday() in
  f () >>= fun r ->
  let t1 = Unix.gettimeofday() in
  begin
    let d = t1 -. t0 in
    if d > 2.0
    then Lwt_io.eprintlf "%s took %2.2f" (f_msg()) d
    else Lwt.return ()
  end
  >>= fun () ->
  Lwt.return r

let _get (client:Arakoon_client.client) max_n k_size t0 oc =
  let k0 = String.make (k_size - 8 ) '_' in
  let rec loop n =
    if n = max_n
    then Lwt.return ()
    else
      begin
        _progress t0 n 10000 oc >>= fun () ->
        let i = Random.int max_n in
        let key = _cat k0 i
        in
        check_timing
          (fun () -> client # get key )
          (fun () -> Printf.sprintf "get %i" n)
        >>= fun _ ->
        loop (n+1)
      end
  in
  loop 0

let _get_transactions (client:Arakoon_client.client)
    max_n t k_size _v_size
    (t0:float) oc=
  let n_transactions = (max_n + t -1) / t in
  let k0 = String.make (k_size -8) '_' in
  let rec loop_t i =
    if i = n_transactions
    then Lwt.return ()
    else
      begin
        let rec build acc j =
          if j = t
          then acc
          else
            let i = Random.int max_n in
            let key = _cat k0 i in
            build (key::acc) (j+1)
        in
        let keys = build [] 0 in
        _progress t0 i 1000 oc >>= fun () ->
        check_timing
          (fun () -> client # multi_get keys)
          (fun () -> Printf.sprintf "multi_get %i" i)
        >>= fun _ ->
        loop_t (i+1)
      end
  in
  loop_t 0



let _fill client max_n k_size v_size (t0:float) (oc:Lwt_io.output_channel) =
  let k0 = String.make (k_size - 8) '_' in
  let v0 = String.make (v_size - 8) 'x' in
  let rec loop n =
    if n = max_n
    then Lwt.return ()
    else
      begin
        _progress t0 n 10000 oc >>= fun () ->
        let key = _cat k0 n
        and value = _cat v0 n in
        check_timing
          (fun () ->client # set key value)
          (fun () ->Printf.sprintf "set %i" n)
        >>= fun () ->
        loop (n+1)
      end
  in
  loop 0

let _fill_transactions client max_n tx_size k_size v_size (t0:float) oc =
  Lwt_io.fprintlf oc "(started @ %f)" (Unix.gettimeofday()) >>= fun () ->
  let n_transactions = (max_n + tx_size -1) / tx_size in
  let k0 = String.make (k_size -8) '_' in
  let v0 = String.make (v_size -8) 'x' in
  let rec loop_t i =
    if i = n_transactions then Lwt.return ()
    else
      begin
        let rec build acc j =
          if j = tx_size then acc
          else
            let c = i * tx_size + j in
            let value = _cat v0 c in
            let u = Set (_cat k0 c, value) in
            build (u::acc) (j+1)
        in
        _progress t0 i 1000 oc >>= fun () ->
        let s = build [] 0 in
        check_timing
          (fun () -> client # sequence s)
          (fun () -> Printf.sprintf "sequence %i" i)
        >>= fun () ->
        loop_t (i+1)
      end
  in
  loop_t 0

let calc_range_keys max_n =
  let first = Random.int max_n - 10000 in
  let last = first + Random.int 10000 in
  (first,last)

let _range (client:Arakoon_client.client) max_n k_size t0 oc =
  Lwt_io.fprintlf oc "(started @ %f: range)" (Unix.gettimeofday()) >>= fun () ->
  let k0 = String.make (k_size - 8) '_' in
  let rec loop i =
    if i = max_n
    then Lwt.return ()
    else
      begin
        let first,last = calc_range_keys max_n in
        let first_key = _cat k0 first in
        let last_key = _cat k0 last in
        _progress t0 i 10000 oc >>= fun () ->
        check_timing
          (fun () -> client # range
            (Some first_key) true
            (Some last_key) true 1000)
          (fun () -> Printf.sprintf "range %i %s %s" i first_key last_key)
        >>= fun _keys ->
        loop (i+1)
      end
  in
  loop 0

let _range_entries (client: Arakoon_client.client) max_n k_size t0 oc =
 Lwt_io.fprintlf oc "(started @ %f: range_entries)"
   (Unix.gettimeofday()) >>= fun () ->
  let k0 = String.make (k_size - 8) '_' in
  let rec loop i =
    if i = max_n
    then Lwt.return ()
    else
      begin
        let first,last = calc_range_keys max_n in
        let first_key = _cat k0 first in
        let last_key = _cat k0 last in
        _progress t0 i 10000 oc >>= fun () ->
        check_timing
          (fun () -> client # range_entries
            ~consistency:Consistent
            ~first:(Some first_key) ~finc:true
            ~last:(Some last_key) ~linc:true ~max:1000)
          (fun () -> Printf.sprintf "range_entries %i %s %s"
            i first_key last_key)
        >>= fun _kvs ->
        loop (i+1)
      end
  in
  loop 0


let _time
      (x:(float -> Lwt_io.output_channel-> unit Lwt.t))
      (oc:Lwt_io.output_channel)=
  let t0 = Unix.gettimeofday() in
  Lwt.catch
    (fun () -> x t0 oc)
    (fun ex ->
       Lwt_io.fprintlf oc "Exception occured: %S"
         (Printexc.to_string ex) >>=fun () ->
       Lwt.fail ex
    )
  >>= fun () ->
  let t1 = Unix.gettimeofday () in
  Lwt.return (t1 -. t0)



let benchmark
      ?(value_size=10)
      ?(key_size=12)
      ?(tx_size=100)
      ?(max_n = 1000 * 1000)
      ~with_c
      n_clients
      scenario
  =
  Lwt_io.printlf "going to run following scenario:" >>= fun () ->
  Lwt_list.iter_s Lwt_io.printl scenario >>= fun () ->
  Lwt_io.printl "--" >>= fun () ->
  let v_size = if value_size < 10 then 10 else value_size in
  let k_size = if key_size < 12 then 12 else key_size in
  let phase_0 client oc =
    client # who_master () >>= fun master ->
    Lwt_io.fprintlf oc "Master %s; size=%i"
      (Log_extra.string_option2s master)
      v_size
  in
  let phase_1 client oc =
    Lwt_io.fprintlf oc "%i 'sets' key_size=%i bytes; value_size=%i bytes"
      max_n k_size v_size >>= fun () ->
    _time (_fill client max_n k_size v_size) oc >>= fun d ->
    Lwt_io.fprintlf oc "\nfill %i took: %f" max_n d  >>= fun () ->
    Lwt.return ()
  in
  let phase_2 client oc =
    Lwt_io.fprintlf oc "\n\n%i 'sets' in transactions of size %i"
      max_n tx_size >>= fun () ->
    _time (_fill_transactions client max_n tx_size k_size v_size) oc >>= fun d ->
    Lwt_io.fprintlf oc "\nfill_transactions %i took : %f" max_n d
  in
  let phase_3 client oc =
    Lwt_io.fprintlf oc "\n\n%i `gets` (random) key_size=%i value_size=%i "
      max_n k_size v_size >>= fun () ->
    _time (_get client max_n k_size) oc >>= fun d ->
    Lwt_io.fprintlf oc "\nget of %i values (random keys) took: %f" max_n d
  in
  let phase_4 client oc =
    Lwt_io.fprintlf oc
      "\n\n%i `multiget` (random, batch size=%i) key_size=%i value_size=%i"
      max_n tx_size k_size v_size >>= fun ()->
    _time (_get_transactions client max_n tx_size k_size v_size) oc >>= fun d ->
    Lwt_io.fprintlf oc
      "\n%i `multigets` (random) key_size=%i value_size =%i tx_size = %i took %f"
      max_n k_size v_size tx_size d
  in
  let phase_5 client oc =
    Lwt_io.fprintlf oc "%i `range` queries; key_size = %i"
      max_n k_size >>= fun () ->
    _time (_range client max_n k_size) oc >>= fun d ->
    Lwt_io.fprintlf oc "\nrandom ranges took %f" d
  in
  let phase_6 client oc =
    Lwt_io.fprintlf oc "\n\n%i `range_entries` queries; key_size = %i%!"
      max_n k_size >>= fun () ->
    _time (_range_entries client max_n k_size) oc >>= fun d ->
    Lwt_io.fprintlf oc "\nrandom `range_entries` queries took %f" d
  in
  let do_one phase fn =
    Lwt_io.printlf "do_one %s" fn >>= fun () ->
    Lwt_io.with_file
      ~flags:[Unix.O_WRONLY;Unix.O_APPEND;Unix.O_CREAT]
      ~mode:Lwt_io.output
      fn
      (fun oc ->
         with_c (fun (client:Arakoon_client.client) -> phase client oc)
      )
    >>= fun () ->
    Lwt.return ()


  in
  let names =
    let rec build acc = function
      | 0 -> List.rev acc
      | n -> let a = "c" ^ (string_of_int n) in build (a :: acc) (n-1)
    in
    build [] n_clients
  in
  let ts ph = List.map (fun name -> do_one ph name) names in
  let phase_map = [
    ("master" , phase_0); ("set", phase_1);
    ("set_tx", phase_2); ("get", phase_3); ("multi_get", phase_4);
    ("range", phase_5);("range_entries", phase_6)
  ]
  in

  let phase_of_name n =
    try List.assoc n phase_map
    with Not_found -> failwith (Printf.sprintf "scenario cannot contain %S" n)
  in
  let phases = List.map phase_of_name scenario in
  Lwt_list.iter_s (fun phase -> Lwt.join (ts phase)) phases


let default_scenario = ["master"; "set"; "set_tx";
                        "get"; "multi_get";
                        "range"; "range_entries";]
