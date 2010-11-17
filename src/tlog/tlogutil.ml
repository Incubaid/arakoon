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

open Log_extra
open Tlogcommon
open Update
open Tlogreader
open Lwt

let rec processEntryQueue q f =
  let (i,msg) = Queue.pop q in
  f i msg;
  if not (Queue.is_empty q) then processEntryQueue q f


let processLastEntries processF ic nrOfEntries =
  let entries = Queue.create () in
  let tReader = new tlogReader ic in
  Lwt.catch
    ( fun () ->
      let rec loop () =
	tReader # readNextEntry () >>= fun (i, update) ->
	Queue.push (i,update) entries;
	let qSize = Queue.length entries in
	if qSize > nrOfEntries && nrOfEntries <> -1  then ignore ( Queue.pop entries );
	loop () >>= fun () ->
	Lwt.return ()
      in loop ()
    )
    ( function
      | End_of_file ->
        processEntryQueue entries processF;
        Lwt.return()
      | exn ->
        Lwt_log.error_f ~exn "Unexpected exception" >>= fun () ->
        Lwt.fail exn
  )


let printEntry oc i update =
  Lwt_io.printlf "%06Ld: %s" i (Update.string_of update)

let printLastEntries oc =
  processLastEntries ( printEntry oc )

