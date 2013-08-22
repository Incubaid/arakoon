type 'a t = Ssl.context

let create_client_context proto = Ssl.create_context proto Ssl.Client_context
let create_server_context proto = Ssl.create_context proto Ssl.Server_context
let create_both_context proto = Ssl.create_context proto Ssl.Both_context

open Lwt

module Lwt = struct
  let ssl_connect fd ctx =
    Lwt_ssl.ssl_connect fd ctx >>= fun lwt_s ->
    match Lwt_ssl.ssl_socket lwt_s with
      | None -> Lwt.fail (Failure "Typed_ssl.Lwt.ssl_connect: unexpected plain socket")
      | Some s -> Lwt.return (s, lwt_s)

  let ssl_accept = Lwt_ssl.ssl_accept
end
