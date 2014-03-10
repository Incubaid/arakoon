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

let load_scenario ~tls cfg_name x =
  let f cn =
    Client_main.with_master_client ~tls
      cfg_name
      (fun client ->
         let rec loop i =
           if i = 1000
           then Lwt.return ()
           else
             begin
               Lwt_io.printlf "%04i: %i" cn i >>= fun () ->
               let key = Printf.sprintf "%i:%i_key" cn i in
               client # set key key >>= fun () ->
               Lwt_unix.sleep 0.1   >>= fun () ->
               loop (i + 1)
             end
         in
         loop 0
      )
  in
  let cnis =
    let rec loop  acc = function
      | 0 -> acc
      | i -> loop (i :: acc) (i-1)
    in
    loop [] x
  in
  Lwt_list.iter_p f cnis

let main ~tls cfg_name n = Lwt_main.run (load_scenario ~tls cfg_name n);0
