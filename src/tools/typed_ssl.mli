type 'a t

val create_client_context : Ssl.protocol -> [ `Client ] t
val create_server_context : Ssl.protocol -> [ `Server ] t
val create_both_context : Ssl.protocol -> [ `Client | `Server ] t

val embed_socket : Unix.file_descr -> 'a t -> Ssl.socket

val use_certificate : 'a t -> string -> string -> unit
val set_verify : 'a t -> Ssl.verify_mode list -> Ssl.verify_callback option -> unit
val set_client_CA_list_from_file : [> `Server ] t -> string -> unit
val load_verify_locations : 'a t -> string -> string -> unit

module Lwt : sig
  val ssl_connect : Lwt_unix.file_descr -> [> `Client ] t -> (Ssl.socket * Lwt_ssl.socket) Lwt.t
  val ssl_accept : Lwt_unix.file_descr -> [> `Server ] t -> (Ssl.socket * Lwt_ssl.socket) Lwt.t
end
