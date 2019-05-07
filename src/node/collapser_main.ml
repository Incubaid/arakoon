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


let collapse_remote ~tls ~tcp_keepalive ip port cluster_id n =
  let t () =
    begin
      if n < 1
      then Lwt.fail (Failure ("n should be >= 1"))
      else Lwt.return ()
    end >>= fun () ->
    let address = Network.make_address ip port in
    let collapse conn =
      Remote_nodestream.make_remote_nodestream cluster_id conn
      >>= fun (client:Remote_nodestream.nodestream) ->
      client # collapse n >>= fun () ->
      Lwt.return 0
    in
    Lwt.catch
      (fun () -> Client_main.with_connection ~tls ~tcp_keepalive address collapse)
      (fun exn -> Logger.fatal ~exn "remote_collapsing_failed"
        >>= fun () -> Lwt.return (-1)
      )
  in
  Lwt_main.run (t () )

let collapse_local make_config node_id n_tlogs =
  let t =
    Lwt.catch
      (fun() ->
        let open Lwt.Infix in
        make_config () >>= fun cfg ->
        Collapser.collapse_out_of_band cfg node_id n_tlogs ~cluster_id:cfg.Node_cfg.Node_cfg.cluster_id >>= fun () ->
        Lwt.return 0

      )
      (fun exn ->
        Logger.fatal ~exn "collapse_local failed"
        >>= fun () ->
        Lwt.return 1
      )
  in
  Lwt_main.run t
