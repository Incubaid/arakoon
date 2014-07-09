module type Protocol = sig
    type ('req, 'res) t

    type some_t = Some_t : (_, _) t -> some_t

    type tag
    val tag_from_channel : Lwt_io.input_channel -> tag Lwt.t
    val tag_to_channel : Lwt_io.output_channel -> tag -> unit Lwt.t

    val command_map : (some_t * tag) list

    module SerDes : sig
        type 'a t

        val from_channel : Lwt_io.input_channel -> 'a t -> 'a Lwt.t
        val to_channel : Lwt_io.output_channel -> 'a t -> 'a -> unit Lwt.t
    end

    val serdes : ('req, 'res) t -> ('req SerDes.t * 'res SerDes.t)
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
              let (req, res) = Protocol.serdes cmd in
              Protocol.tag_to_channel oc tag >>= fun () ->
              Protocol.SerDes.to_channel oc req r >>= fun () ->
              Lwt_io.flush oc >>= fun () ->
              Protocol.SerDes.from_channel ic res
          end
end

module Server = struct
    type 'a result = Continue of 'a
                   | Close of 'a
                   | Die

    module Make(P : sig
        include Protocol

        val handle : ('req, 'res) t -> 'req -> 'res result Lwt.t
    end) : sig
        exception Unknown_tag of P.tag

        val session :  Lwt_io.input_channel
                    -> Lwt_io.output_channel
                    -> unit Lwt.t
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
                let (req, res) = P.serdes cmd in
                P.SerDes.from_channel ic req >>= fun req' ->
                P.handle cmd req' >>= fun res' ->
                match res' with
                  | Continue r
                  | Close r ->
                      P.SerDes.to_channel oc res r >>= fun () ->
                      Lwt_io.flush oc >>= fun () ->
                      Lwt.return res'
                  | Die -> Lwt.return res'
            in

            let rec loop () =
                P.tag_from_channel ic >>= fun tag ->
                match lookup tag with
                  | None -> (* TODO Send to client *) Lwt.fail (Unknown_tag tag)
                  | Some (P.Some_t c) -> begin
                      go c >>= function
                        | Continue _ -> loop ()
                        | Close _
                        | Die -> Lwt.return ()
                  end
            in
            loop ()
    end
end
