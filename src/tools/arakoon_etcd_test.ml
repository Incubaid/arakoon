(*
Copyright 2016 iNuron NV

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

open Lwt.Infix

let fail_with_etcd_process_failure f =
  Lwt.catch
    (fun () ->
     f () >>= fun _ ->
     Lwt.fail_with "f did not throw!")
    (function
      | Arakoon_etcd.ProcessFailure _ ->
         Lwt.return ()
      | exn -> Lwt.fail exn)

let test_etcd_down () =
  let peers = [ "127.0.0.1", 2390 ] in
  fail_with_etcd_process_failure
    (fun () -> Arakoon_etcd.retrieve_value peers "fs") >>= fun () ->
  fail_with_etcd_process_failure
    (fun () -> Arakoon_etcd.store_value peers "ci" "sfdi") >>= fun () ->
  Lwt.return ()

let test_with_etcd f =
  Lwt_process.with_process_none
    ("", [| "pkill"; "etcd"; |])
    (fun _ -> Lwt.return ()) >>= fun () ->
  Lwt_process.with_process_none
    ("", [| "rm"; "-rf"; "default.etcd"; |])
    (fun _ -> Lwt.return ()) >>= fun () ->
  let peers = [ "127.0.0.1", 2379 ] in
  let etcd =
    Lwt_process.open_process_none
      ("", [| "etcd";
              "--listen-client-urls"; "http://127.0.0.1:2379";
              "--advertise-client-urls"; "http://127.0.0.1:2379";
             |])
  in
  Lwt_unix.sleep 1. >>= fun () ->
  Lwt.finalize
    (fun () -> f peers)
    (fun () ->
     etcd # terminate;
     Lwt.return ())

let test_get_set () =
  test_with_etcd
    (fun peers ->
     let key = "/test/etcd/get_set" in
     let test value =
       Arakoon_etcd.store_value peers key value >>= fun () ->
       Arakoon_etcd.retrieve_value peers key >>= fun value' ->
       Printf.printf "expected = %S, actual = %S\n"
                     value value';
       assert (value = value');
       Lwt.return ()
     in
     let values =
       [ "lala";
         "lala\r\n";
         "la la";
         "la\r\n la";
         "la ' \\ \' \" \n \" la";
         "";
       ]
     in
     Lwt_list.iter_s
       test
       values)

let test_get_non_existing_key () =
  test_with_etcd
    (fun peers ->
     fail_with_etcd_process_failure
       (fun () ->
        Arakoon_etcd.retrieve_value
          peers
          (Random.int64 Int64.max_int |> Int64.to_string)))

open OUnit

let suite =
  "etcd_test" >:::
    let t x () = Lwt_main.run (x ()) in
    [ "test_get_set" >:: t test_get_set;
      "test_etcd_down" >:: t test_etcd_down;
      "test_get_non_existing_key" >:: t test_get_non_existing_key;
    ]
