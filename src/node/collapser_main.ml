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

let collapse tlog_dir n_tlogs =
  let t () =
    Tlc2.get_tlog_names tlog_dir >>= fun entries -> 
    let rec take_n acc entries i= 
      if i = 0 
      then List.rev acc
      else 
	match entries with
	  | [] -> failwith "not enough entries to collapse"
	  | hd::tl -> take_n (hd::acc) tl (i-1)
    in
    let to_collapse = take_n [] entries n_tlogs in
    Lwt_io.printl "going to collapse" >>= fun () ->
    Lwt_list.iter_s (fun e -> Lwt_io.printl ("\t" ^e)) to_collapse >>= fun () ->
    Collapser.collapse_many tlog_dir to_collapse Collapser.head_name  >>= fun () ->
    Lwt.return 0
  in
  Lwt_main.run (t())
