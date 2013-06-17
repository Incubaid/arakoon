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
open Lwt_buffer

let _ =
  let rec produce item period q = 
    Lwt_io.printlf "produced: %s" item >>= fun () ->
    Lwt_buffer.add item q >>= fun () -> 
    Lwt_unix.sleep period >>= fun () -> 
    produce item period q
  in
  let capacity = Some 5 in
  let q0 = Lwt_buffer.create ~capacity () in
  let q1 = Lwt_buffer.create ~capacity () in
  let q2 = Lwt_buffer.create ~capacity () in
  let p0 () = produce "p0_stuff" 1.0 q0 in
  let p1 () = produce "p1_stuff" 1.0 q1 in
  let p2 () = produce "p2_stuff" 1.0 q2 in

  let rec c qs = 
    let consume q  =
      Lwt_unix.sleep 3.0 >>= fun () ->
      Lwt_buffer.take q >>= fun item -> 
      Lwt_io.printlf "consumed %S" item
    in
    let ready q = Lwt_buffer.wait_for_item q  >>= fun () -> Lwt.return q
    in
    let waiters = List.map ready qs in
    Lwt.npick waiters >>= fun ready_qs ->
    
    Lwt_list.iter_s consume ready_qs >>= fun () ->
    c qs
  
    
  in
  let qs = [q2;q1;q0] in
  let main () = join [p0();
		      p1();
		      p2();
		      c qs;
		     ]
  in
  Lwt_extra.run (main());;
