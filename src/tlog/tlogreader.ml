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
open Update
open Lwt_log
open Printf

class tlogReader ic =

  object (self)

  val mutable lastKnownGoodEndPos  = 0L
  val mutable lastKnownGoodStartPos  = 0L
  val mutable lastKnownGoodI = Sn.start


  method closeChannel () =
    Lwt_io.close ic

  method getLastKnownGoodI () = lastKnownGoodI

  method getLastKnownGoodEndPos () = lastKnownGoodEndPos

  method getLastKnownGoodStartPos () = lastKnownGoodStartPos

  method validateTlog () =
    let previous = ref None in
    Lwt.catch
      ( fun() ->
	let rec loop () =
	  self # readNextEntry () >>= fun ( i, cmd ) ->
	  begin
	    match !previous with
	      | Some(pi, _ ) ->
		if i = pi || i = Sn.succ pi 
		then Lwt.return ()
		else
		  Llio.lwt_failfmt
		    "Inconsistent consecutive entries in tlog (i1: %s i2: %s)"
		    (Sn.string_of pi)
                    (Sn.string_of i )
	      | None -> Lwt.return ()
	  end
	  >>= fun () ->
	  previous := Some(i,cmd);
	  loop ()
        in
	loop ()
      )
      ( function
	| End_of_file ->
          begin
	    Lwt_log.debug "End of file reached" >>= fun () ->
	    let validity =
	      match !previous with
		| None -> TlogValidIncomplete
		| Some (i,cmd) ->
		  if ( Sn.rem i ( Sn.of_int !tlogEntriesPerFile ) ) = Sn.of_int (!tlogEntriesPerFile -1 )
		  then TlogValidComplete
		  else TlogValidIncomplete
            in Lwt.return (validity, !previous)
	  end
	| exn -> Lwt.fail exn)


  method readNextEntry ()  =
    read_entry ic >>= fun ( i , update ) ->
    lastKnownGoodI <- i;
    lastKnownGoodEndPos <- Lwt_io.position ic;
    Lwt.return( i, update )

  

  method fold lowerI higherI a0 (f:'a -> Sn.t * Update.t -> unit Lwt.t) =
    let next () =
      Lwt.catch
	(fun () ->
	  self # readNextEntry () >>= fun t ->
	  Lwt.return (Some t)
	)
	(function
	  | End_of_file -> (Lwt_io.close ic >>= fun () -> Lwt.return None )
	  | exn -> Lwt.fail exn)
    in
    let rec skip_until () =
      next () >>= function
	| None -> Lwt.return None
	| Some (i,update) ->
	  if i < lowerI
	  then skip_until ()
	  else Lwt.return (Some (i, update) )
    in
    let rec _fold a (pi,pu) =
      next () >>= function
	| None ->
	  begin
	    f a (pi, pu) 
	  end
	    
	| Some (i,update) ->
	  (begin
	    if i > pi then
	      f a (pi, pu)
	    else
	      Lwt.return a
	   end
	   >>= fun a' ->
	   if (i > higherI) 
	   then Lwt.return a
	   else _fold a' (i, update)
	  )
    in skip_until () >>= function
      | None ->  Lwt.return a0
      | Some (i0,u0) ->
	_fold a0 (i0,u0)

  method iterate (lowerI:Sn.t) (higherI:Sn.t) (f:Sn.t * Update.t -> unit Lwt.t) =
    let acc_f () b = f b in
    self # fold lowerI higherI () acc_f
      
end
