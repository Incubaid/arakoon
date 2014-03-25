(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

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
open OUnit
open Master_type
open Node_cfg.Node_cfg

let section = Logger.Section.main

let stop = ref false

let setup tn master base () =
  let lease_period = 10 in
  stop := false;
  let make_config () = Node_cfg.Node_cfg.make_test_config ~base 3 master lease_period in
  let t0 = Node_main.test_t make_config "t_arakoon_0" stop >>= fun _ -> Lwt.return () in
  let t1 = Node_main.test_t make_config "t_arakoon_1" stop >>= fun _ -> Lwt.return () in
  let t2 = Node_main.test_t make_config "t_arakoon_2" stop >>= fun _ -> Lwt.return () in
  let all_t = [t0;t1;t2] in
  Lwt.return (tn, make_config (), all_t)

let teardown (tn, _, all_t) =
  stop := true;
  Lwt.join all_t

let _with_master_admin ~tls (tn, cluster_cfg, _) f =
  let sp = float(cluster_cfg._lease_period) *. 1.2 in
  Lwt_unix.sleep sp >>= fun () -> (* let the cluster reach stability *)
  Client_main.find_master ~tls cluster_cfg >>= fun master_name ->
  Logger.info_f_ "master=%S" master_name >>= fun () ->
  let master_cfg =
    List.hd
      (List.filter (fun cfg -> cfg.node_name = master_name) cluster_cfg.cfgs)
  in
  let host,port = List.hd master_cfg.ips , master_cfg.client_port in
  let sa = Network.make_address host port in
  let cid = cluster_cfg.cluster_id in
  Client_main.with_connection ~tls sa
    (fun conn ->
       Remote_nodestream.make_remote_nodestream cid conn >>= fun admin ->
       f cluster_cfg master_name admin
    )

let _drop_master ~tls cluster_cfg master_name admin =
  Logger.info_ "drop_master scenario" >>= fun () ->
  admin # drop_master () >>= fun () ->
  Client_main.find_master ~tls cluster_cfg >>= fun new_master ->
  Logger.info_f_ "new? master = %s" new_master >>= fun () ->
  OUnit.assert_bool "master should have been changed" (new_master <> master_name);
  Lwt.return ()

let drop_master ~tls tpl = _with_master_admin ~tls tpl (_drop_master ~tls)



let make_suite base name w =
  let make_el n base f = n >:: w n base f in
  name >:::
    [make_el "drop_master" base (drop_master ~tls:None);
    ]


let suite =
  let w tn base f = Extra.lwt_bracket (setup tn Elected base) f teardown in
  make_suite 8000 "drop_master" w
