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

    let to_string f = function
      | Ok a -> Printf.sprintf "Ok (%s)" (f a)
      | No_magic s -> Printf.sprintf "No_magic %S" s
      | No_hello s -> Printf.sprintf "No_hello %S" s
      | Not_master s -> Printf.sprintf "Not_master %S" s
      | Not_found s -> Printf.sprintf "Not_found %S" s
      | Wrong_cluster s -> Printf.sprintf "Wrong_cluster %S" s
      | Assertion_failed s -> Printf.sprintf "Assertion_failed %S" s
      | Read_only s -> Printf.sprintf "Read_only %S" s
      | Outside_interval s -> Printf.sprintf "Outside_interval %S" s
      | Going_down s -> Printf.sprintf "Going_down %S" s
      | Not_supported s -> Printf.sprintf "Not_supported %S" s
      | No_longer_master s -> Printf.sprintf "No_longer_master %S" s
      | Bad_input s -> Printf.sprintf "Bad_input %S" s
      | Inconsistent_read s -> Printf.sprintf "Inconsistent_read %S" s
      | Userfunction_failure s -> Printf.sprintf "Userfunction_failure %S" s
      | Max_connections s -> Printf.sprintf "Max_connections %S" s
      | Unknown_failure s -> Printf.sprintf "Unknown_failure %S" s

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

    module SerDes = struct
        type _ t =
          | String : string t
          | Unit : unit t
          | Option : 'a t -> 'a option t
          | Tuple2 : ('a t * 'b t) -> ('a * 'b) t
          | Tuple3 : ('a t * 'b t * 'c t) -> ('a * 'b * 'c) t
          | Result : 'a t -> ('a Result.t) t
          | Consistency : Arakoon_client.consistency t
          | Bool : bool t

        let from_channel =
            let rec loop : type a. Lwt_io.input_channel -> a t -> a Lwt.t = fun ic -> function
              | String -> Llio.input_string ic
              | Unit -> Lwt.return ()
              | Tuple2 (a, b) -> begin
                  loop ic a >>= fun a' ->
                  loop ic b >>= fun b' ->
                  Lwt.return (a', b')
              end
              | Tuple3 (a, b, c) -> begin
                  loop ic a >>= fun a' ->
                  loop ic b >>= fun b' ->
                  loop ic c >>= fun c' ->
                  Lwt.return (a', b', c')
              end
              | Result a -> Result.from_channel (fun ic' -> loop ic' a) ic
              | Consistency -> Common.input_consistency ic
              | Bool -> Llio.input_bool ic
              | Option t -> begin
                  Llio.input_bool ic >>= function
                    | false -> Lwt.return None
                    | true -> loop ic t >>= fun v -> Lwt.return (Some v)
              end
            in
            loop

        let to_channel =
            let rec loop : type a. Lwt_io.output_channel -> a t -> a -> unit Lwt.t = fun oc t a ->
                match t with
                  | String -> Llio.output_string oc a
                  | Unit -> Lwt.return ()
                  | Tuple2 (a', b') -> begin
                      let (a'', b'') = a in
                      loop oc a' a'' >>= fun () ->
                      loop oc b' b''
                  end
                  | Tuple3 (a', b', c') -> begin
                      let (a'', b'', c'') = a in
                      loop oc a' a'' >>= fun () ->
                      loop oc b' b'' >>= fun () ->
                      loop oc c' c''
                  end
                  | Result a' -> Result.to_channel (fun oc' a'' -> loop oc' a' a'') oc a
                  | Consistency -> Common.output_consistency oc a
                  | Bool -> Llio.output_bool oc a
                  | Option t -> match a with
                      | None -> Llio.output_bool oc false
                      | Some v -> Llio.output_bool oc true >>= fun () -> loop oc t v
            in
            loop
    end

    type ('req, 'res) t =
      | Ping : ((string * string), string Result.t) t
      | Who_master : (unit, string option Result.t) t
      | Exists : ((Arakoon_client.consistency * string), bool Result.t) t
      | Get : ((Arakoon_client.consistency * string), string Result.t) t
      | Set : ((string * string), unit Result.t) t
      | Delete : (string, unit Result.t) t
      | Test_and_set : ((string * string option * string option), string option Result.t) t
      | Expect_progress_possible : (unit, bool Result.t) t
      | User_function : ((string * string option), string option Result.t) t
      | Assert : ((Arakoon_client.consistency * string * string option), unit Result.t) t
      | Assert_exists : ((Arakoon_client.consistency * string), unit Result.t) t
      | Replace : ((string * string option), string option Result.t) t
      | Nop : (unit, unit Result.t) t
      | Flush_store : (unit, unit Result.t) t
      | Get_txid : (unit, Arakoon_client.consistency Result.t) t

    let serdes : type r s. (r, s) t -> (r SerDes.t * s SerDes.t) =
        let open SerDes in
        function
          | Ping -> Tuple2 (String, String), Result String
          | Who_master -> Unit, Result (Option String)
          | Exists -> Tuple2 (Consistency, String), Result Bool
          | Get -> Tuple2 (Consistency, String), Result String
          | Set -> Tuple2 (String, String), Result Unit
          | Delete -> String, Result Unit
          | Test_and_set -> Tuple3 (String, Option String, Option String), Result (Option String)
          | Expect_progress_possible -> Unit, Result Bool
          | User_function -> Tuple2 (String, Option String), Result (Option String)
          | Assert -> Tuple3 (Consistency, String, Option String), Result Unit
          | Assert_exists -> Tuple2 (Consistency, String), Result Unit
          | Replace -> Tuple2 (String, Option String), Result (Option String)
          | Nop -> Unit, Result Unit
          | Flush_store -> Unit, Result Unit
          | Get_txid -> Unit, Result Consistency

    type some_t = Some_t : (_, _) t -> some_t

    let mAGIC = Common._MAGIC
    let mASK = Common._MASK

    type tag = Int32.t

    exception Invalid_magic of tag

    let tag_from_channel ic =
        Llio.input_int32 ic >>= fun masked ->
        let magic = Int32.logand masked mAGIC in
        if magic <> mAGIC
        then
            (* TODO Return No_magic to client *)
            Lwt.fail (Invalid_magic magic)
        else
            Lwt.return (Int32.logand masked mASK)

    let tag_to_channel oc t =
        let masked = Int32.logor t mAGIC in
        Llio.output_int32 oc masked

    let command_map = [ Some_t Ping, 0x01l
                      ; Some_t Who_master, 0x02l
                      ; Some_t Exists, 0x07l
                      ; Some_t Get, 0x08l
                      ; Some_t Set, 0x09l
                      ; Some_t Delete, 0x0al
                      ; Some_t Test_and_set, 0x0dl
                      ; Some_t Expect_progress_possible, 0x12l
                      ; Some_t User_function, 0x15l
                      ; Some_t Assert, 0x16l
                      ; Some_t Assert_exists, 0x29l
                      ; Some_t Replace, 0x33l
                      ; Some_t Nop, 0x41l
                      ; Some_t Flush_store, 0x42l
                      ; Some_t Get_txid, 0x43l
                      ]
end

module Client = Rpc.Client(Protocol)
