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
open Update
open OUnit
open Tlogcommon

let test_old_format () =
  let do_it () = 
    let fn = "./data/005.tlc" in
    let print_entry a entry = 
      let i = Entry.i_of entry in
      let v = Entry.v_of entry in
      let is = Sn.string_of i in 
      let vs = Value.value2s v in
      Lwt_io.eprintlf "%s:%s" is vs >>= fun () ->
      Lwt.return a 
    in
    let f ic = 
      let a0 = () in
      let first = Sn.of_int 500015 in
      let start = Sn.of_int 500015 in
      let stop  = Some (Sn.of_int 500175) in
      Tlogreader2.O.fold ic ~index:None
        start ~first stop a0 print_entry >>= fun () ->
      Lwt.return () 
    in
    Lwt_io.with_file ~mode:Lwt_io.input fn f
  in
  Lwt_extra.run (do_it ())


let suite = "tlogreader2" >::: ["old_format" >:: test_old_format]
















  
