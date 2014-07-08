module Result = struct
    type 'a t = Ok of 'a
              | No_magic of string
              | No_hello of string
              | Not_master of string
              | Not_found of string
              | Wrong_cluster of string
              | Assertion_failed of string
              | Read_only of string
              | Outside_interval of string
              | Going_down of string
              | Not_supported of string
              | No_longer_master of string
              | Bad_input of string
              | Inconsistent_read of string
              | Userfunction_failure of string
              | Max_connections of string
              | Unknown_failure of string

    let no_magic s = No_magic s
    and no_hello s = No_hello s
    and not_master s = Not_master s
    and not_found s = ((Not_found s) : 'a t)
    and wrong_cluster s = Wrong_cluster s
    and assertion_failed s = Assertion_failed s
    and read_only s = Read_only s
    and outside_interval s = Outside_interval s
    and going_down s = Going_down s
    and not_supported s = Not_supported s
    and no_longer_master s = No_longer_master s
    and bad_input s = Bad_input s
    and inconsistent_read s = Inconsistent_read s
    and userfunction_failure s = Userfunction_failure s
    and max_connections s = Max_connections s
    and unknown_failure s = Unknown_failure s

    open Lwt

    let to_channel f oc =
        let code = Llio.output_int32 oc in
        let error c s = code c >>= fun () -> Llio.output_string oc s in
        function
          | Ok a -> code 0x00l >>= fun () -> f oc a
          | No_magic s -> error 0x01l s
          | No_hello s -> error 0x03l s
          | Not_master s -> error 0x04l s
          | Not_found s -> error 0x05l s
          | Wrong_cluster s -> error 0x06l s
          | Assertion_failed s -> error 0x07l s
          | Read_only s -> error 0x08l s
          | Outside_interval s -> error 0x09l s
          | Going_down s -> error 0x10l s
          | Not_supported s -> error 0x20l s
          | No_longer_master s -> error 0x21l s
          | Bad_input s -> error 0x26l s
          | Inconsistent_read s -> error 0x80l s
          | Userfunction_failure s -> error 0x81l s
          | Max_connections s -> error 0xfel s
          | Unknown_failure s -> error 0xffl s

    let from_channel f ic =
        let error g =
            Lwt.map g (Llio.input_string ic)
        in

        Llio.input_int32 ic >>= function
          | 0x00l -> f ic >>= fun r -> Lwt.return (Ok r)
          | 0x01l -> error no_magic
          | 0x02l -> error no_hello
          | 0x04l -> error not_master
          | 0x05l -> error not_found
          | 0x06l -> error wrong_cluster
          | 0x07l -> error assertion_failed
          | 0x08l -> error read_only
          | 0x09l -> error outside_interval
          | 0x10l -> error going_down
          | 0x20l -> error not_supported
          | 0x21l -> error no_longer_master
          | 0x26l -> error bad_input
          | 0x80l -> error inconsistent_read
          | 0x81l -> error userfunction_failure
          | 0xfel -> error max_connections
          | 0xffl -> error unknown_failure
          | _ -> error unknown_failure
end

module Protocol = struct
    open Lwt

    type ('req, 'res) t =
      | Ping : ((string * string), string Result.t) t
      | Who_master : (unit, string option Result.t) t

    type some_t = Some_t : (_, _) t -> some_t

    exception Invalid_magic

    let mAGIC = Common._MAGIC
    let mASK = Common._MASK

    type tag = Int32.t

    let tag_from_channel ic =
        Llio.input_int32 ic >>= fun masked ->
        let magic = Int32.logand masked mAGIC in
        if magic <> mAGIC
        then
            Lwt.fail Invalid_magic
        else
            Lwt.return (Int32.logand masked mASK)

    let tag_to_channel oc t =
        let masked = Int32.logor t mAGIC in
        Llio.output_int32 oc masked

    let command_map = [ Some_t Ping, 0x01l
                      ; Some_t Who_master, 0x02l
                      ]

    let io_for_command : type r s. (r, s) t -> (r, s) Rpc.IO.t = fun t -> match t with
      | Ping -> begin
          let req_to_channel oc (client_id, cluster_id) =
              Llio.output_string oc client_id >>= fun () ->
              Llio.output_string oc cluster_id
          and req_from_channel ic =
              Llio.input_string ic >>= fun client_id ->
              Llio.input_string ic >>= fun cluster_id ->
              Lwt.return (client_id, cluster_id)
          and res_to_channel = Result.to_channel Llio.output_string
          and res_from_channel = Result.from_channel Llio.input_string in
          Rpc.IO.({ req_to_channel; req_from_channel; res_to_channel; res_from_channel })
      end
      | Who_master -> begin
          let req_to_channel _ () = Lwt.return ()
          and req_from_channel _ = Lwt.return ()
          and res_to_channel = Result.to_channel Llio.output_string_option
          and res_from_channel = Result.from_channel Llio.input_string_option in
          Rpc.IO.({ req_to_channel; req_from_channel; res_to_channel; res_from_channel })
      end
end

module Client = Rpc.Client(Protocol)
