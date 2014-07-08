module IO = struct
    type ('req, 'res) t = { req_from_channel : Lwt_io.input_channel -> 'req Lwt.t
                          ; req_to_channel : Lwt_io.output_channel -> 'req -> unit Lwt.t
                          ; res_from_channel : Lwt_io.input_channel -> 'res Lwt.t
                          ; res_to_channel : Lwt_io.output_channel -> 'res -> unit Lwt.t
                          }
end

module type Protocol = sig
    type ('req, 'res) t

    type some_t = Some_t : (_, _) t -> some_t

    type tag
    val tag_from_channel : Lwt_io.input_channel -> tag Lwt.t
    val tag_to_channel : Lwt_io.output_channel -> tag -> unit Lwt.t

    val command_map : (some_t * tag) list

    val io_for_command : ('req, 'res) t -> ('req, 'res) IO.t
end

module Client(Protocol : Protocol) : sig
    exception Unknown_command of Protocol.some_t

    val request :  Lwt_io.input_channel
                -> Lwt_io.output_channel
                -> ('req, 'res) Protocol.t
                -> 'req
                -> 'res Lwt.t
end= struct
    open Lwt

    exception Unknown_command of Protocol.some_t

    module M = Map.Make(struct type t = Protocol.some_t let compare = compare end)
    let command_map = List.fold_left (fun m (k, v) -> M.add k v m) M.empty Protocol.command_map

    let lookup c =
        try
            let t = M.find c command_map in
            Some t
        with Not_found ->
            None

    let request ic oc cmd r =
        match lookup (Protocol.Some_t cmd) with
          | None -> Lwt.fail (Unknown_command (Protocol.Some_t cmd))
          | Some tag -> begin
              let io = Protocol.io_for_command cmd in
              Protocol.tag_to_channel oc tag >>= fun () ->
              io.IO.req_to_channel oc r >>= fun () ->
              Lwt_io.flush oc >>= fun () ->
              io.IO.res_from_channel ic
          end
end

module Server(P : sig
    include Protocol

    val handle : ('req, 'res) t -> 'req -> 'res Lwt.t
end) : sig
    exception Unknown_tag of P.tag

    val session :  Lwt_io.input_channel
                -> Lwt_io.output_channel
                -> 'a Lwt.t
end = struct
    open Lwt

    exception Unknown_tag of P.tag

    module M = Map.Make(struct type t = P.tag let compare = compare end)
    let command_map = List.fold_left (fun m (k, v) -> M.add v k m) M.empty P.command_map

    let lookup t =
        try
            let c = M.find t command_map in
            Some c
        with Not_found ->
            None

    let session ic oc =
        let go  cmd =
            let io = P.io_for_command cmd in
            io.IO.req_from_channel ic >>= fun req ->
            P.handle cmd req >>= fun res ->
            io.IO.res_to_channel oc res >>= fun () ->
            Lwt_io.flush oc
        in

        let rec loop () =
            P.tag_from_channel ic >>= fun tag ->
            match lookup tag with
              | None -> Lwt.fail (Unknown_tag tag)
              | Some (P.Some_t c) -> go c >>= loop
        in
        loop ()
end
