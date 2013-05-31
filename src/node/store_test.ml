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
open Store

let section = Logger.Section.main

module type Test_STORE =
sig
  include STORE

  val _get_j : t -> int
  val _with_transaction_lock : t -> (transaction_lock -> 'a Lwt.t) -> 'a Lwt.t
  val _insert_update : t -> Update.t -> key_or_transaction -> update_result Lwt.t
end

module S = (val (module Make(Batched_store.Local_store) : Test_STORE with type ss = Batched_store.Local_store.t))

let _dir_name = "/tmp/store_test"

let setup () = 
  Logger.info_ "Store_test.setup" >>= fun () ->
  let ignore_ex f = 
    Lwt.catch 
      f
      (fun exn -> Logger.warning_ ~exn "ignoring")
  in
  ignore_ex (fun () -> File_system.rmdir _dir_name) >>= fun () ->
  ignore_ex (fun () -> File_system.mkdir  _dir_name 0o750 )

let teardown () = 
  Logger.info_ "Store_test.teardown" >>= fun () ->
  Lwt.catch
    (fun () ->
      File_system.lwt_directory_list _dir_name >>= fun entries ->
      Lwt_list.iter_s (fun i -> 
	let fn = _dir_name ^ "/" ^ i in
        Lwt_unix.unlink fn) entries 
    )
    (fun exn -> Logger.debug_ ~exn "ignoring" )
    >>= fun () ->
  Logger.debug_ "end of teardown"

let with_store name f =
  let db_name = _dir_name ^ "/" ^ name ^ ".db" in
  S.make_store db_name >>= fun store ->
  f store >>= fun () ->
  S.close store

let assert_k_v k v (store:S.t) =
  S.get store k >>= fun av ->
  if v <> av then failwith "not as expected" else Lwt.return ()

let assert_not_exists k (store:S.t) =
  S.exists store k >>= fun exists ->
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
  let get_next_i store = match S.consensus_i store with
    | None -> Sn.of_int 0
    | Some i -> Sn.succ i in
  let do_asserts store =
    Lwt_list.iter_s
      (fun (value, asserts) ->
        S.safe_insert_value store (get_next_i store) value >>= fun _ ->
        let j = S._get_j store in
        if j <> 0 then failwith "j is not 0";
        Lwt_list.iter_s
          (fun assert' -> assert' store)
          asserts)
      value_asserts in
  Logger.info_ "applying updates without surrounding transaction" >>= fun () ->
  with_store "tsiv" (fun store ->
    do_asserts store)

let test_safe_insert_value_with_partial_value_update () =
  with_store "tsivwpvu" (fun store ->
    let k = "key" in
    let u1 = Update.TestAndSet(k, Some "value1", Some "illegal")
    and u2 = Update.TestAndSet(k, None, Some "value1")
    and u3 = Update.Set("key2", "bla") in
    let paxos_value = Value.create_client_value [u1;u2;u3] false in
    S._with_transaction_lock store (fun k -> S._insert_update store u1 (Store.Key k)) >>= fun _ ->
    S._with_transaction_lock store (fun k -> S._insert_update store u2 (Store.Key k)) >>= fun _ ->

    let j = S._get_j store in
    if j = 0 then failwith "j is 0";

    S.get store k >>= fun value ->
    if value <> "value1" then failwith "key should have value1 as value";

    S.safe_insert_value store (Sn.of_int 0) paxos_value >>= fun _ ->

    let j = S._get_j store in
    if j <> 0 then failwith "j is not 0";

    S.get store k >>= fun value ->
    if value <> "value1" then failwith "illegal value in store";
    Lwt.return ()
  )


let suite =
  let w f = lwt_bracket setup f teardown in
  "store" >:::[
    "safe_insert_value" >:: w test_safe_insert_value;
    "safe_insert_value_with_partial_value_update" >:: w test_safe_insert_value_with_partial_value_update;
  ]

