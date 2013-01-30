open Lwt
open Modules
open Common
open Mp

module VCommon(S:Core.STORE) (A:MP_ACTION_DISPATCHER) = struct
  module D = Mp_driver.MPDriver(A)


  let handle_exception oc exn=
    let rc, msg, is_fatal, close_socket = match exn with
      | XException(Arakoon_exc.E_NOT_FOUND, msg) -> Arakoon_exc.E_NOT_FOUND,msg, false, false
      | XException(Arakoon_exc.E_ASSERTION_FAILED, msg) ->
        Arakoon_exc.E_ASSERTION_FAILED, msg, false, false
      | XException(rc, msg) -> rc,msg, false, true
      | Not_found -> Arakoon_exc.E_NOT_FOUND, "Not_found", false, false
      | Server.FOOBAR -> Arakoon_exc.E_UNKNOWN_FAILURE, "unkown failure", true, true
      | _ -> Arakoon_exc.E_UNKNOWN_FAILURE, "unknown failure", false, true
    in
    Lwt_log.error_f "Exception during client request (%s)" (Printexc.to_string exn) >>= fun () ->
    Arakoon_exc.output_exception oc rc msg >>= fun () ->
    begin
	  if close_socket
	  then Lwt_log.debug "Closing client socket" >>= fun () -> Lwt_io.close oc
	  else Lwt.return ()
    end >>= fun () ->
    if is_fatal
    then Lwt.fail exn
    else Lwt.return close_socket
      
  let response_ok_unit oc =
    Llio.output_int32 oc 0l >>= fun () ->
    Lwt.return false
      

  let response_rc_string oc rc string =
    Llio.output_int32 oc rc >>= fun () ->
    Llio.output_string oc string >>= fun () ->
    Lwt.return false

  
  let do_unit_update driver q =
    D.push_cli_req driver q >>= function
      | Core.VOID -> Lwt.return ()
      | Core.FAILURE (rc, msg) -> Lwt.fail (Common.XException(rc,msg))
      | Core.VALUE v -> failwith "Expected unit, not value"

  let admin_set driver k m_v = 
    let u = Core.ADMIN_SET(k, m_v) in
    do_unit_update driver u

  let admin_get store k = 
    S.admin_get store k >>= function
      | None -> Lwt.fail (Common.XException(Arakoon_exc.E_NOT_FOUND, k))
      | Some v -> Lwt.return v

  let who_master store = 
    S.get_meta store >>= fun meta ->
    Lwt.return (Core.extract_master_info meta)
      
  let am_i_master store me = 
    who_master store >>= fun mo ->
    let r = match mo with
      | Some m -> m = me
      | _      -> false
    in
    Lwt.return r
        
end


