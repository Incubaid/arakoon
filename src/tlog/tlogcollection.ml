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


open Tlogwriter
open Tlogcommon
open Update
open Tlogreader
open Lwt


let getTlogFilenameFromI i =
  let fileAsSn = Sn.div i (Sn.of_int !tlogEntriesPerFile ) in
  Printf.sprintf "%s%s" (Sn.string_of fileAsSn) tlogExtension

(* TODO: s/int/Sn.t/ for tlog_collection *)

class type tlog_collection = object
  method validate: unit -> (tlogValidity * Sn.t option) Lwt.t
  method validate_last_tlog: unit -> (tlogValidity * Sn.t option) Lwt.t 
  method iterate: Sn.t -> Sn.t -> (Sn.t * Update.t -> unit Lwt.t) -> unit Lwt.t
  method log_update: Sn.t -> Update.t -> Tlogwriter.writeResult Lwt.t
  method get_last_update: Sn.t -> Update.t option Lwt.t
  method close : unit -> unit Lwt.t
end


class file_tlog_collection tlogDir =
  object(self: #tlog_collection)

    val mutable _prevWriter = None
    val mutable _prevWriterName = ""
    val mutable last_update = None
    val mutable last_tlog_validated = false

    method private get_tlog_base_names () =
      lwt_directory_list tlogDir >>= fun dirEntries ->
      Lwt_list.filter_p (fun e -> Lwt.return ( Str.string_match tlogFileRegex e 0 ) ) dirEntries 

    method private get_last_tlog_entry base_names =
      
      let bn_cmp cur_max baseName =
         let dotPos = String.index baseName '.' in
         let baseNameNumStr = String.sub baseName 0 dotPos in
	 let baseNameNum = int_of_string baseNameNumStr in
         match cur_max with
         | None -> Some baseNameNum
         | Some c_max -> Some (max c_max baseNameNum)
      in
      Lwt.return ( List.fold_left bn_cmp None base_names )
      

    method validate_last_tlog () =
      Lwt_log.info "tlogCollection :: validate_last_tlog" >>= fun () ->
      self # get_tlog_base_names () >>= fun logEntries ->
      self # get_last_tlog_entry logEntries >>= fun lastEntry ->
      match lastEntry with 
        | None -> self # validate_base_name_list []
        | Some entry -> 
            begin
            let last_base_name = Printf.sprintf "%d.tlog" entry in
            Lwt_log.debug_f "Validating %s" last_base_name >>= fun() ->
            self # validate_base_name_list [ last_base_name ]
            end
      
    method validate () =
      (* Build the list of all tlog entries in the collection *)
      Lwt_log.info_f "tlocCollection :: validate" >>= fun () ->
      self # get_tlog_base_names () >>= fun logEntries ->
      self # validate_base_name_list logEntries 

    method private validate_base_name_list logEntries =
      let f baseName =
	Lwt.catch
	  (fun () ->
            let fullPath = Filename.concat tlogDir baseName in
            let dotPos = String.index baseName '.' in
            let baseNameNumStr = String.sub baseName 0 dotPos in
	    let baseNameNum = int_of_string baseNameNumStr in
            let f ic =
              let tReader = new tlogReader ic in
              tReader # validateTlog ()
            in
            Lwt_io.with_file ~mode:Lwt_io.input fullPath f >>= fun validWithLastOp ->
            let lastOp = snd validWithLastOp in
	    let r =
              match lastOp with
		| Some op ->
		  let validWithI = ( fst validWithLastOp, Some (fst( op )) ) in
		  let () = last_update <- lastOp in
		  baseNameNum, validWithI
		| None -> baseNameNum, (TlogInvalid, None)
	    in Lwt.return r
	  )
	  (fun exn -> Lwt_log.info_f "%s seems to be invalid log" baseName
		      >>= fun () -> Lwt.return (-1,(TlogInvalid, None)) )

      in
      Lwt_list.map_s f logEntries >>= fun validationList ->
      let sortedRes = List.sort compare validationList in
      let chainedValidation prevState listElem =
        let r =
          match prevState with
            | (TlogInvalid, i) -> prevState
            | (TlogValidIncomplete, i) -> ( TlogInvalid, i )
            | (TlogValidComplete, i) ->
              match i with
               | None ->
                  snd listElem
               | Some i ->
                  if (Sn.succ i ) =
		    Sn.mul ( Sn.of_int ( fst listElem) )
		      ( Sn.of_int !tlogEntriesPerFile )
	          then snd listElem
                  else (TlogInvalid, None)
	in
	Lwt.return r
      in
      Lwt_list.fold_left_s
	chainedValidation
	(TlogValidComplete, None)
	sortedRes >>= fun x ->
        let () = last_tlog_validated <- true in
        Lwt.return x

    method iterate (start_i:Sn.t) (consensus_i:Sn.t) (f:Sn.t * Update.t -> unit Lwt.t) =
      let (tReader:tlogReader) = self # _get_tlog_reader start_i in
      tReader # iterate start_i consensus_i f >>= fun finished ->
      tReader # closeChannel () >>= fun () ->
      let lastReadI = tReader # getLastKnownGoodI () in
      let epf = !tlogEntriesPerFile in
      if Sn.rem lastReadI (Sn.of_int epf) = Sn.of_int (epf -1)
      then
	begin
        (* Catch the (valid) situation when the file does not exist *)
          Lwt.catch
	    ( fun () -> self # iterate ( Sn.succ lastReadI) consensus_i f)
	    ( function
              | Unix.Unix_error (Unix.ENOENT, _, _ ) -> Lwt.return ()
	      | exn ->
		Lwt_log.error_f ~exn
		  "Could not open tlog file i %s" (Sn.string_of lastReadI) >>= fun () ->
		Lwt.fail exn
            )
	end
      else Lwt.return ()


    method log_update (i: Sn.t) update =
      self # _getTlogWriter i >>= fun w ->
      w # log_update i update >>= fun res ->
      let () = if res = WRSuccess then
	last_update <- (Some (i,update))
      in Lwt.return res

    method get_last_update (i: Sn.t) =
      begin
      if last_tlog_validated
      then Lwt.return ()
      else self # validate_last_tlog () >>= fun _ -> Lwt.return ()
      end 
      >>= fun () ->
      match last_update with
	| None ->
	  begin
	    Lwt_log.info_f "get_value: no update logged yet" >>= fun () ->
	    Lwt.return None
	  end
	| Some (i',x) ->
	  begin
	    if i = i'
	    then Lwt.return (Some x)
	    else
	      (Lwt_log.info_f "get_value: i(%s) is not latest update" (Sn.string_of i) >>= fun () ->
	       Lwt.return None)
	  end

    method private _get_tlog_reader (i: Sn.t)  =
      let tlogFileBasename = getTlogFilenameFromI i in
      let fullPath = Filename.concat tlogDir tlogFileBasename in
      let ic = Lwt_io.open_file  ~mode:Lwt_io.input fullPath in
      new tlogReader ic

     method private _getTlogWriter i =
       let lastValidatedI = ref None in
       let tlogFileBasename = getTlogFilenameFromI i in
       if _prevWriterName = tlogFileBasename
       then (
         match _prevWriter with
           | Some twriter -> Lwt.return twriter
           | None -> Lwt.fail ( Failure "Missing tlog writer" )
       )
       else
         (
           let tlogFileName = Filename.concat tlogDir tlogFileBasename in
           Lwt.catch
             (fun () ->
               let update_offset ic =
                 let tReader = new tlogReader ic in
                 tReader # validateTlog () >>= fun _ ->
                 let r = tReader # getLastKnownGoodEndPos () in
                 lastValidatedI := Some( tReader # getLastKnownGoodI () );
                 Lwt.return r
               in
               Lwt_io.with_file ~mode:Lwt_io.input tlogFileName
                 update_offset
             )
             (function
               | Unix.Unix_error (Unix.ENOENT, msg, param) ->
                 (* If the tlog does not exist yet errno will be set to ENOENT.
                    This is the only valid case where we will not
		    re-raise *)
                 Lwt.return 0L
               | exn ->
                 Lwt_log.error_f ~exn "Could not open tlog file %S"
                   tlogFileName >>= fun () ->
                 Lwt.fail exn
             )
	       >>= fun writeOffset ->

           begin
             match _prevWriter with
               | Some obj ->
                 (* Check if the previous tlog is full before returning
the writer object *)
                 let prevWriterI = obj # getLastI () in
                 begin
                   if (Sn.rem prevWriterI (Sn.of_int !tlogEntriesPerFile) ) <> ( Sn.of_int (!tlogEntriesPerFile - 1) )
                      && i <> Sn.succ prevWriterI
                   then Llio.lwt_failfmt "Invalid request for a writer. i's are not consecutive. lastWrite:%s requested:%s" (Sn.string_of prevWriterI) (Sn.string_of i)
                   else Lwt.return ()
                 end >>= fun () ->
                 obj # closeChannel ()
                   | None ->
                     begin
                       if Sn.rem i (Sn.of_int !tlogEntriesPerFile) = 0L && writeOffset = 0L
                       then Lwt.return()
                       else (
                         match !lastValidatedI with
                           | None -> Llio.lwt_failfmt "Invalid request for a writer. Value for i (%s) should be first in new tlog file" (Sn.string_of i)
                           | Some oldI ->
                             if (oldI <> i) && ( oldI <> Sn.pred i )
                             then Llio.lwt_failfmt "Invalid request for a writer. Values for i ( %s and %s ) are not consecutive" (Sn.string_of i) (Sn.string_of oldI)
                             else Lwt.return ()
                       )
                     end
           end >>= fun () ->
           let outStream = Lwt_io.open_file
             ~flags:[Unix.O_CREAT; Unix.O_WRONLY ]
             ~perm:0o644
             ~mode:Lwt_io.output tlogFileName
           in
           Lwt_io.set_position outStream writeOffset >>= fun () ->
           let tWriter = new Tlogwriter.tlogWriter outStream i in
           _prevWriter <- Some tWriter ;
           _prevWriterName <- tlogFileBasename ;
           Lwt.return tWriter
         )

     method close () = 
       match _prevWriter with
	 | None -> Lwt.return ()
	 | Some w -> w # closeChannel()
  end

let make_file_tlog_collection tlog_dir =
  let col = new file_tlog_collection tlog_dir in
  Lwt.return col
