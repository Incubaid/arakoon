
type rc =
  | E_OK
  | E_NO_MAGIC
  | E_TOO_MANY_DEAD_NODES
  | E_NO_HELLO
  | E_NOT_MASTER
  | E_NOT_FOUND
  | E_UNKNOWN_FAILURE

val int32_of_rc : rc -> int32

val rc_of_int32: int32 -> rc

exception Exception of rc * string

val output_exception : Lwt_io.output_channel -> rc -> string -> unit Lwt.t

