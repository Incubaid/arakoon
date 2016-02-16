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

let test_get_set () =
  let t () =
    let etcd =
      Lwt_process.open_process_none
        ("", [| "etcd";
                "--listen-client-urls"; "http://127.0.0.1:2379";
                "--advertise-client-urls"; "http://127.0.0.1:2379";
               |])
    in
    Lwt_unix.sleep 0.3 >>= fun () ->
    Lwt.finalize
      (fun () ->
       let peers = [ "127.0.0.1", 2379 ] in
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
         ]
       in
       Lwt_list.iter_s
         test
         values)
      (fun () ->
       etcd # terminate;
       Lwt.return ())
  in
  Lwt_main.run (t ())

open OUnit

let suite = "etcd_test" >::: [
    "test_get_set" >:: test_get_set;
  ]
