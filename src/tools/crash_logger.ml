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

let maybe_circ_buf = ref None
let msg_cnt = ref 0

let setup_crash_log crash_file_prefix =
  let max_crash_log_size = 1000 in
  let circ_buf = 
  begin
    match !maybe_circ_buf with
      | None -> 
        let new_circ_buf = Lwt_sequence.create() in
        maybe_circ_buf := Some new_circ_buf ;
        new_circ_buf
      | Some cb -> cb
  end
  in
  let level_to_string lvl =
  begin
	  match lvl with
	    | Lwt_log.Debug -> "debug"
	    | Lwt_log.Info -> "info"
	    | Lwt_log.Notice -> "notice"
	    | Lwt_log.Warning -> "warning"
	    | Lwt_log.Error -> "error" 
	    | Lwt_log.Fatal -> "fatal"
  end 
  in
  
  let rec remove_n_elements n =
    match n with
      | 0  -> ()
      | n' -> 
        let _ = Lwt_sequence.take_opt_l circ_buf in
        let () = (msg_cnt := !msg_cnt - 1) in
        remove_n_elements (n-1)
  in
  
  let add_msg lvl msg  =
    let () = (msg_cnt := !msg_cnt + 1) in
    let full_msg = Printf.sprintf "%Ld: %s: %s" (Int64.of_float ( Unix.time() )) lvl msg in
    let _ = Lwt_sequence.add_r full_msg circ_buf  in
    lvl 
  in
  
  let add_to_crash_log section level msgs =
    let new_msg_cnt = List.length msgs in
    let total_msgs = !msg_cnt + new_msg_cnt in
    let () =
      begin
        if  total_msgs > max_crash_log_size 
        then
          remove_n_elements (total_msgs - max_crash_log_size)
        else
          ()
      end
    in
    let _ = List.fold_left add_msg (level_to_string level) msgs in
    Lwt.return () 
  in 
  
  let dump_crash_log () =  
    let dump_msgs oc =
      let dump_msg msg =
        let line = msg ^ "\n" in
        ignore( Lwt_io.write oc line )
      in
      Lwt.return (Lwt_sequence.iter_l dump_msg circ_buf)
    in 
    let crash_file_path = crash_file_prefix in
    Lwt_io.with_file ~mode:Lwt_io.output crash_file_path dump_msgs
  in 
  
  let fake_close () = Lwt.return () in
  
  (add_to_crash_log, fake_close, dump_crash_log) 


let setup_default_logger file_log_level file_log_path crash_log_prefix =
  
  let file_section = Lwt_log.Section.make "file_section" in
  Lwt_log.Section.set_level file_section file_log_level;
  Lwt_log.Section.set_level Lwt_log.Section.main Lwt_log.Debug;
  let file_logger = Lwt_log.file
      ~template:"$(date): $(level): $(message)"
      ~mode:`Append ~file_name:file_log_path ()
  in
  
  let (log_crash_msg, close_crash_log, dump_crash_log) = setup_crash_log crash_log_prefix in 
  
  let add_log_msg section level msgs =
    let log_file_msg msg = ignore(Lwt_log.log ~section:file_section ~logger:file_logger ~level msg) in 
    let () = List.iter log_file_msg msgs in 
    log_crash_msg section level msgs
  in
    
  let close_default_logger () =
    Lwt_log.close file_logger >>= fun () ->
    close_crash_log ()
  in
  
  let default_logger = Lwt_log.make add_log_msg close_default_logger in
  Lwt_log.default := default_logger;
  dump_crash_log
   

      


