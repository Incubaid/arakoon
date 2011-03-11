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

let _progress t0 n step =
  if n > 0 && n mod step = 0 then 
    begin
      let ti = Unix.gettimeofday() in
      let delta = ti -. t0 in
      let f = if n mod (step * 5) = 0 
	then Lwt_io.printlf
	else Lwt_io.printf
      in
      f "%9i (% 4.0fs)" n delta
    end
  else
    Lwt.return()

let _get (client:Arakoon_client.client) t0 max_n =
  let rec loop n = 
    if n = max_n 
    then Lwt.return ()
    else
      begin
	_progress t0 n 10000 >>= fun () ->
	let i = Random.int max_n in
	let key = _cat "key" i
	in client # get key >>= fun _ ->
	loop (n+1)
      end
  in
  loop 0

let _get_transactions client t0 max_n t size = 
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
	client # multi_get keys >>= fun values ->
	loop_t (i+1)
      end
  in
  loop_t 0
      


let _fill client t0 max_n size= 
  let v0 = String.make (size - 8) 'x' in
  let rec loop n =
    if n = max_n
    then Lwt.return ()
    else
      begin
	_progress t0 n 10000 >>= fun () ->
	let key = _cat "key" n
	and value = _cat v0 n in
	client # set key value >>= fun () ->
	loop (n+1)
      end
  in
  loop 0

let _fill_transactions client t0 max_n tx_size size=
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
	let s = build [] 0 in
	client # sequence s >>= fun () ->
	loop_t (i+1)
      end
  in
  loop_t 0


let _range client ()  =
  let first = _cat "key" 1
  and last = _cat "key" 9999
  in
  Lwt_io.printlf "range %s true %s true -1" first last >>= fun () ->
  client # range (Some first) true (Some last) true (-1) >>= fun keys ->
  Lwt_io.printlf "#keys %i" (List.length keys)


let benchmark ?(size=10) ?(tx_size=100) (client:Arakoon_client.client) =
  let sz = if size < 10 then 10 else size in
  client # who_master () >>= fun master ->
  Lwt_io.printlf "Master %s; size=%i" 
    (Log_extra.string_option_to_string master)
    sz
  >>= fun () ->
  let max_n = 1000 * 1000 in
  (* last_entries () >>= fun () -> *)
  Lwt_io.printlf "going to do %i 'sets' of %i bytes" max_n sz >>= fun () ->
  let t0 = Unix.gettimeofday () in
  _fill client t0 max_n sz >>= fun () ->
  let t1 = Unix.gettimeofday () in
  Lwt_io.printlf "\nfill %i took: %f" max_n (t1 -. t0) >>= fun () ->
  Lwt_io.printlf "\n\ngoing to do %i 'sets' in transactions of size %i"  max_n tx_size
  >>= fun () ->
  _fill_transactions client t1 max_n tx_size sz >>= fun () ->
  let t2 = Unix.gettimeofday() in
  Lwt_io.printlf "\nfill_transactions %i took : %f" max_n (t2 -.t1) >>= fun() ->
  Lwt_io.printlf "\n\ngoing to %i gets of random keys" max_n >>= fun () ->
  _get client t2 max_n >>= fun () ->
  let t3 = Unix.gettimeofday() in
  Lwt_io.printlf "\nget of %i values (random keys) took: %f" max_n (t3 -.t2) 
  >>= fun() ->
  _get_transactions client t3 max_n tx_size sz >>= fun () ->
  let t4 = Unix.gettimeofday () in
  Lwt_io.printlf "\nmultiget of %i values (random keys) in transactions of size %i took %f"
    max_n tx_size (t4 -. t3) >>= fun () ->
  Lwt_io.printlf "range" >>= fun () ->
  _range client ()  >>= fun () ->
  (* ... *)
  Lwt.return ()












