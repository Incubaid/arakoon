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

open Client_cfg
open OUnit

let test_parsing () = 
  let lines = [
    "[global]";
   "cluster_id = sturdy";
   "cluster = sturdy_0,sturdy_1, sturdy_2  ";
   "\n";
   "[sturdy_0]";
   "client_port = 7080";
   "name = sturdy_0";
   "ip = 127.0.0.1";
   "\n";
   "[sturdy_1]";
   "client_port = 7081";
   "name = sturdy_1";
   "ip = 127.0.0.1";
   "\n";
   "[sturdy_2]";
   "ip = 127.0.0.1";
   "client_port = 7082";
   "name = sturdy_2"]
  in
  let contents = List.fold_left (fun acc v -> acc ^ "\n" ^ v) "" lines in
  let fn = "/tmp/client_cfg_test.ml" in
  let oc = open_out "/tmp/client_cfg_test.ml" in
  let () = output_string oc contents in
  let () = close_out oc in
  let cfg = ClientCfg.from_file "global" fn in
  let sa0 = ClientCfg.get cfg "sturdy_0" in
  OUnit.assert_equal sa0 (["127.0.0.1"],7080)

let suite = "client_cfg" >:::[
  "parsing" >:: test_parsing;
]
