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
open Arakoon_remote_client
open Arakoon_client
open Network

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

let _get (client:Arakoon_client.client) max_n t0 oc =
  let rec loop n =
    if n = max_n
    then Lwt.return ()
    else
      begin
        _progress t0 n 10000 oc >>= fun () ->
        let i = Random.int max_n in
        let key = _cat "key" i
        in client # get key >>= fun _ ->
        loop (n+1)
      end
  in
  loop 0

let _get_transactions (client:Arakoon_client.client) max_n t size (t0:float) oc=
  let n_transactions = (max_n + t -1) / t in
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
            let key = _cat "key" i in
            build (key::acc) (j+1)
        in
        let keys = build [] 0 in
        _progress t0 i 1000 oc >>= fun () ->
        client # multi_get keys >>= fun values ->
        loop_t (i+1)
      end
  in
  loop_t 0



let _fill client max_n size (t0:float) (oc:Lwt_io.output_channel) =
  let v0 = String.make (size - 8) 'x' in
  let rec loop n =
    if n = max_n
    then Lwt.return ()
    else
      begin
        _progress t0 n 10000 oc >>= fun () ->
        let key = _cat "key" n
        and value = _cat v0 n in
        client # set key value >>= fun () ->
        loop (n+1)
      end
  in
  loop 0

let _fill_transactions client max_n tx_size size (t0:float) oc =
  Lwt_io.fprintlf oc "(started @ %f)" (Unix.gettimeofday()) >>= fun () ->
  let n_transactions = (max_n + tx_size -1) / tx_size in
  let v0 = String.make  (size -8) 'x' in
  let rec loop_t i =
    if i = n_transactions then Lwt.return ()
    else
      begin
        let rec build acc j =
          if j = tx_size then acc
          else
            let c = i * tx_size + j in
            let value = _cat v0 c in
            let u = Set (_cat "key" c, value) in
            build (u::acc) (j+1)
        in
        _progress t0 i 1000 oc >>= fun () ->
        let s = build [] 0 in
        client # sequence s >>= fun () ->
        loop_t (i+1)
      end
  in
  loop_t 0


let _range (client:Arakoon_client.client) ()  =
  let first = _cat "key" 1
  and last = _cat "key" 9999
  in
  Lwt_io.printlf "range %s true %s true -1" first last >>= fun () ->
  client # range (Some first) true (Some last) true (-1) >>= fun keys ->
  Lwt_io.printlf "#keys %i" (List.length keys)

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
      ?(size=10)
      ?(tx_size=100)
      ?(max_n = 1000 * 1000)
      ~with_c
      n_clients
  =
  let sz =
    if size < 10 then 10 else size
  in
  let phase_0 client oc =
    client # who_master () >>= fun master ->
    Lwt_io.fprintlf oc "Master %s; size=%i"
      (Log_extra.string_option2s master)
      sz
  in
  let phase_1 client oc =
    Lwt_io.fprintlf oc "going to do %i 'sets' of %i bytes" max_n sz >>= fun () ->
    _time (_fill client max_n sz) oc >>= fun d ->
    Lwt_io.fprintlf oc "\nfill %i took: %f" max_n d  >>= fun () ->
    Lwt.return ()
  in
  let phase_2 client oc =
    Lwt_io.fprintlf oc "\n\ngoing to do %i 'sets' in transactions of size %i"
      max_n tx_size >>= fun () ->
    _time (_fill_transactions client max_n tx_size sz) oc >>= fun d ->
    Lwt_io.fprintlf oc "\nfill_transactions %i took : %f" max_n d
  in
  let phase_3 client oc =
    Lwt_io.fprintlf oc "\n\ngoing to %i gets of random keys" max_n >>= fun () ->
    _time (_get client max_n ) oc >>= fun d ->
    Lwt_io.fprintlf oc "\nget of %i values (random keys) took: %f" max_n d
  in
  let phase_4 client oc =
    (* ???? >>= fun () -> *)
    _time (_get_transactions client max_n tx_size sz) oc >>= fun d ->
    Lwt_io.fprintlf oc
      "\nmultiget of %i values (random keys) in transactions of size %i took %f"
      max_n tx_size d
  in
  let phase_5 client oc =
    Lwt_io.fprintlf oc "range" >>= fun () ->
    _range client ()
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
  Lwt.join (ts phase_0) >>= fun () ->
  Lwt.join (ts phase_1) >>= fun () ->
  Lwt.join (ts phase_2) >>= fun () ->
  Lwt.join (ts phase_3) >>= fun () ->
  Lwt.join (ts phase_4) >>= fun () ->
  Lwt.join (ts phase_5) >>= fun () ->
  Lwt.return ()
