(*
bufferke = inbox
local state, a la object?
process van ne message


abstract base class
-> implementeert loopke, run(_ignored) method, thread opvragen
*)


open Lwt
open Lwt_buffer

class virtual ['a] actor (msg_buf : 'a Lwt_buffer.t) =
  object (self)
    method virtual process_message : 'a -> unit
    method run () : unit Lwt.t =
      Lwt_buffer.take msg_buf >>= fun msg ->
      self # process_message msg;
      self # run ()
  end
