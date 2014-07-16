module type Protocol = sig
    type ('req, 'res) t

    type some_t = Some_t : (_, _) t -> some_t

    module Type : sig
        type 'a t

        val from_channel : Lwt_io.input_channel -> 'a t -> 'a Lwt.t
        val to_channel : Lwt_io.output_channel -> 'a t -> 'a -> unit Lwt.t
    end

    module Tag : sig
        type t

        val from_channel : Lwt_io.input_channel -> t Lwt.t
        val to_channel : Lwt_io.output_channel -> t -> unit Lwt.t

        val compare : t -> t -> int
    end

    val meta : ('req, 'res) t -> ('req Type.t * 'res Type.t)
    val command_map : (some_t * Tag.t) list
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
              let (req, res) = Protocol.meta cmd in
              Protocol.Tag.to_channel oc tag >>= fun () ->
              Protocol.Type.to_channel oc req r >>= fun () ->
              Lwt_io.flush oc >>= fun () ->
              Protocol.Type.from_channel ic res
          end
end

module Server = struct
    module Result = struct
        type 'a t = Continue of 'a
                  | Close of 'a
                  | Die

        let map f = function
          | Continue v -> Continue (f v)
          | Close v -> Close (f v)
          | Die -> Die
    end

    module Make(P : sig
        module Protocol : Protocol

        exception Unknown_tag of Protocol.Tag.t

        val handle : ('req, 'res) Protocol.t -> 'req -> 'res Result.t Lwt.t
        val handle_exception : (Lwt_io.input_channel * Lwt_io.output_channel) -> exn -> unit Result.t Lwt.t
    end) : sig
        val session :  Lwt_io.input_channel
                    -> Lwt_io.output_channel
                    -> unit Lwt.t
    end = struct
        open Lwt

        module M = Map.Make(struct
            type t = P.Protocol.Tag.t
            let compare = P.Protocol.Tag.compare
        end)
        let command_map = List.fold_left (fun m (k, v) -> M.add v k m) M.empty P.Protocol.command_map

        let lookup t =
            try
                let c = M.find t command_map in
                Some c
            with Not_found ->
                None

        let session ic oc =
            let go cmd =
                let (req, res) = P.Protocol.meta cmd in
                P.Protocol.Type.from_channel ic req >>= fun req' ->
                P.handle cmd req' >>= fun res' ->
                match res' with
                  | Result.Continue r
                  | Result.Close r ->
                      P.Protocol.Type.to_channel oc res r >>= fun () ->
                      Lwt_io.flush oc >>= fun () ->
                      Lwt.return res'
                  | Result.Die -> Lwt.return res'
            in

            let rec loop () =
                begin Lwt.catch (fun () ->
                    P.Protocol.Tag.from_channel ic >>= fun tag ->
                    begin match lookup tag with
                      | None -> Lwt.fail (P.Unknown_tag tag)
                      | Some (P.Protocol.Some_t c) -> begin
                          go c >>= fun r -> Lwt.return (Result.map (fun _ -> ()) r)
                      end
                    end)
                    (fun exn -> P.handle_exception (ic, oc) exn)
                end >>= function
                  | Result.Continue _ -> loop ()
                  | Result.Close _
                  | Result.Die -> Lwt.return ()
            in
            loop ()
    end
end
