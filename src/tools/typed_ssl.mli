type 'a t

val create_client_context : Ssl.protocol -> [ `Client ] t
val create_server_context : Ssl.protocol -> [ `Server ] t
val create_both_context : Ssl.protocol -> [ `Client | `Server ] t

module Lwt : sig
  val ssl_connect : Lwt_unix.file_descr -> [> `Client ] t -> (Ssl.socket * Lwt_ssl.socket) Lwt.t
end
