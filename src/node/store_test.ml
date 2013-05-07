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

open OUnit
open Lwt
open Extra
open Update

let _dir_name = "/tmp/store_test"

let setup () = 
  Lwt_log.info "Store_test.setup" >>= fun () ->
  let ignore_ex f = 
    Lwt.catch 
      f
      (fun exn -> Lwt_log.warning ~exn "ignoring")
  in
  ignore_ex (fun () -> File_system.rmdir _dir_name) >>= fun () ->
  ignore_ex (fun () -> File_system.mkdir  _dir_name 0o750 )

let teardown () = 
  Lwt_log.info "Store_test.teardown" >>= fun () ->
  Lwt.catch
    (fun () ->
      File_system.lwt_directory_list _dir_name >>= fun entries ->
      Lwt_list.iter_s (fun i -> 
	let fn = _dir_name ^ "/" ^ i in
        Lwt_unix.unlink fn) entries 
    )
    (fun exn -> Lwt_log.debug ~exn "ignoring" )
    >>= fun () ->
  Lwt_log.debug "end of teardown"

let with_store name f =
  let db_name = _dir_name ^ "/" ^ name ^ ".db" in
  Local_store.make_local_store db_name >>= fun store ->
  f store >>= fun () ->
  store # close ()

let assert_k_v k v (store:Store.store) =
  Store.get store k >>= fun av ->
  if v <> av then failwith "not as expected" else Lwt.return ()

let assert_not_exists k (store:Store.store) =
  store # exists k >>= fun exists ->
  if exists then failwith "key should not exist" else Lwt.return ()

let failing_value =
  let k2 = "key2"
  and v2 = "value2" in
  Value.create_client_value [Update.Sequence [Update.Set(k2, v2);
                                              Update.Assert_exists "notExists"]] false


let value_asserts =
  let k1 = "key1"
  and v1 = "value1" in
  [(Value.create_client_value [Update.Set(k1,v1)] false,
    [assert_k_v k1 v1]);
   (failing_value,
    [assert_not_exists "key2"]);]

let test_safe_insert_value () =
  let get_next_i store = match store # consensus_i () with
    | None -> Sn.of_int 0
    | Some i -> Sn.succ i in
  let do_asserts store =
    Lwt_list.iter_s
      (fun (value, asserts) ->
        Store.safe_insert_value store (get_next_i store) value >>= fun _ ->
        let j = Store.get_j store in
        if j <> 0 then failwith "j is not 0";
        Lwt_list.iter_s
          (fun assert' -> assert' store)
          asserts)
      value_asserts in
  Lwt_log.info "applying updates without surrounding transaction" >>= fun () ->
  with_store "tsiv" (fun store ->
    do_asserts store)

let test_safe_insert_value_with_partial_value_update () =
  with_store "tsivwpvu" (fun store ->
    let k = "key" in
    let u1 = Update.TestAndSet(k, Some "value1", Some "illegal")
    and u2 = Update.TestAndSet(k, None, Some "value1")
    and u3 = Update.Set("key2", "bla") in
    let paxos_value = Value.create_client_value [u1;u2;u3] false in
    store # with_transaction_lock (fun k -> Store._insert_update store u1 (Store.Key k)) >>= fun _ ->
    store # with_transaction_lock (fun k -> Store._insert_update store u2 (Store.Key k)) >>= fun _ ->

    let j = Store.get_j store in
    if j = 0 then failwith "j is 0";

    Store.get store k >>= fun value ->
    if value <> "value1" then failwith "key should have value1 as value";

    Store.safe_insert_value store (Sn.of_int 0) paxos_value >>= fun _ ->

    let j = Store.get_j store in
    if j <> 0 then failwith "j is not 0";

    Store.get store k >>= fun value ->
    if value <> "value1" then failwith "illegal value in store";
    Lwt.return ()
  )

let test_safe_insert_value_with_tx () =
  with_store "tsivwt" (fun store ->
    store # with_transaction (fun tx ->
      (Lwt.catch
         (fun () -> Store.safe_insert_value store ~tx:(Some tx) (Sn.of_int 0) failing_value
                    >>= fun _ -> Lwt.return 0)
         (fun _ -> Lwt.return 1)) >>= fun r ->
      if r = 0
      then failwith "safe_insert_value did not fail while in a transaction"
      else Lwt.return ()))


let suite =
  let w f = lwt_bracket setup f teardown in
  "store" >:::[
    "safe_insert_value" >:: w test_safe_insert_value;
    "safe_insert_value_with_tx" >:: w test_safe_insert_value_with_tx;
    "safe_insert_value_with_partial_value_update" >:: w test_safe_insert_value_with_partial_value_update;
  ]

