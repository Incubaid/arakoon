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
open Tlogcommon
open Common


class tlogWriter oc lastI =

  object (self)

    val mutable lastWrittenI = lastI;

    method getLastI () = lastWrittenI

    method closeChannel () = Lwt_io.close oc

    method log_value i value =
      if isValidSuccessor i lastWrittenI  then
        begin
          write_entry oc i value >>= fun () ->
          Lwt_io.flush oc >>= fun () ->
          let () = lastWrittenI <- i in
          Lwt.return ()
        end
      else
        Llio.lwt_failfmt "invalid successor %s" (Sn.string_of i)


  end
