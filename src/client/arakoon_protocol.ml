external id : 'a -> 'a = "%identity"

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

    let map f = function
      | Ok a -> Ok (f a)
      | No_magic _
      | No_hello _
      | Not_master _
      | Not_found _
      | Wrong_cluster _
      | Assertion_failed _
      | Read_only _
      | Outside_interval _
      | Going_down _
      | Not_supported _
      | No_longer_master _
      | Bad_input _
      | Inconsistent_read _
      | Userfunction_failure _
      | Max_connections _
      | Unknown_failure _ as e -> e

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

module RangeRequest = struct
    type t = { first : string option
             ; finc : bool
             ; last : string option
             ; linc : bool
             ; max : int
             }

    let t ~first ~finc ~last ~linc ~max =
        { first; finc; last; linc; max }

    let to_string t =
        let open To_string in
        record [ "first", option string t.first
               ; "finc", bool t.finc
               ; "last", option string t.last
               ; "linc", bool t.linc
               ; "max", int t.max
               ]
end

module ReversedArray : sig
    type 'a t

    val make : 'a array -> 'a t
    val length : 'a t -> int
    val array : 'a t -> 'a array
end = struct
    type 'a t = 'a array

    let make = id
    let length a = Array.length a
    let array = id
end

module CountedList : sig
    type 'a t

    val make : length:int -> list:'a list -> 'a t
    val of_list : 'a list -> 'a t
    val length : 'a t -> int
    val list : 'a t -> 'a list
end = struct
    type 'a t = int * 'a list

    let make ~length ~list = (length, list)
    let of_list l = (List.length l, l)
    let length (l, _) = l
    let list (_, l) = l
end

module Protocol = struct
    open Lwt

    module Type : sig
        type 'a t

        val to_channel : Lwt_io.output_channel -> 'a t -> 'a -> unit Lwt.t
        val from_channel : Lwt_io.input_channel -> 'a t -> 'a Lwt.t

        val make :  from_channel:(Lwt_io.input_channel -> 'a Lwt.t)
                 -> to_channel:(Lwt_io.output_channel -> 'a -> unit Lwt.t)
                 -> 'a t

        val unit : unit t
        val int : int t
        val int32 : int32 t
        val int64 : int64 t
        val string : string t
        val bool : bool t
        val option : 'a t -> 'a option t
        val tuple2 : 'a t -> 'b t -> ('a * 'b) t
        val tuple3 : 'a t -> 'b t -> 'c t -> ('a * 'b * 'c) t
        val tuple4 : 'a t -> 'b t -> 'c t -> 'd t -> ('a * 'b * 'c * 'd) t
        val list : 'a t -> 'a list t

        val result : 'a t -> 'a Result.t t
        val consistency : Arakoon_client.consistency t
        val range_request : RangeRequest.t t
        val reversed_array : 'a t -> 'a ReversedArray.t t
        val key : Key.t t
        val counted_list : 'a t -> 'a CountedList.t t
        val update : Update.Update.t t
        val statistics : Statistics.Statistics.t t
    end = struct
        type 'a t = { from_channel : (Lwt_io.input_channel -> 'a Lwt.t)
                    ; to_channel : (Lwt_io.output_channel -> 'a -> unit Lwt.t)
                    }

        let make ~from_channel ~to_channel = { from_channel
                                             ; to_channel
                                             }

        let to_channel oc t a = t.to_channel oc a
        let from_channel ic t = t.from_channel ic

        let unit =
            let from_channel _ = Lwt.return ()
            and to_channel _ () = Lwt.return () in
            make ~from_channel ~to_channel

        let string = make ~from_channel:Llio.input_string ~to_channel:Llio.output_string
        let int = make ~from_channel:Llio.input_int ~to_channel:Llio.output_int
        let int32 = make ~from_channel:Llio.input_int32 ~to_channel:Llio.output_int32
        let int64 = make ~from_channel:Llio.input_int64 ~to_channel:Llio.output_int64

        let bool = make ~from_channel:Llio.input_bool ~to_channel:Llio.output_bool

        let option at =
            let from_channel ic =
                from_channel ic bool >>= function
                  | false -> Lwt.return None
                  | true -> begin
                      from_channel ic at >>= fun a ->
                      Lwt.return (Some a)
                  end
            and to_channel oc = function
              | None -> to_channel oc bool false
              | Some a -> begin
                  to_channel oc bool true >>= fun () ->
                  to_channel oc at a
              end
            in
            make ~from_channel ~to_channel

        let tuple2 at bt =
            let from_channel ic =
                from_channel ic at >>= fun a ->
                from_channel ic bt >>= fun b ->
                Lwt.return (a, b)
            and to_channel oc (a, b) =
                to_channel oc at a >>= fun () ->
                to_channel oc bt b
            in
            make ~from_channel ~to_channel
        let tuple3 at bt ct =
            let inner = tuple2 (tuple2 at bt) ct in
            let from_channel ic =
                from_channel ic inner >>= fun ((a, b), c) ->
                Lwt.return (a, b, c)
            and to_channel oc (a, b, c) =
                to_channel oc inner ((a, b), c)
            in
            make ~from_channel ~to_channel
        let tuple4 at bt ct dt =
            let inner1 = tuple2 at bt
            and inner2 = tuple2 ct dt in
            let from_channel ic =
                from_channel ic inner1 >>= fun (a, b) ->
                from_channel ic inner2 >>= fun (c, d) ->
                Lwt.return (a, b, c, d)
            and to_channel oc (a, b, c, d) =
                to_channel oc inner1 (a, b) >>= fun () ->
                to_channel oc inner2 (c, d)
            in
            make ~from_channel ~to_channel

        let list at =
            let from_channel = Llio.input_list (fun ic -> from_channel ic at)
            and to_channel = Llio.output_list (fun oc v -> to_channel oc at v) in
            make ~from_channel ~to_channel

        let result at =
            let from_channel = Result.from_channel (fun ic -> from_channel ic at)
            and to_channel = Result.to_channel (fun oc a -> to_channel oc at a) in
            make ~from_channel ~to_channel

        let consistency =
            let from_channel = Common.input_consistency
            and to_channel = Common.output_consistency in
            make ~from_channel ~to_channel

        let range_request =
            let from_channel ic =
                from_channel ic (option string) >>= fun first ->
                from_channel ic bool >>= fun finc ->
                from_channel ic (option string) >>= fun last ->
                from_channel ic bool >>= fun linc ->
                from_channel ic int >>= fun max ->
                Lwt.return (RangeRequest.t ~first ~finc ~last ~linc ~max)
            and to_channel oc a =
                let open RangeRequest in
                to_channel oc (option string) a.first >>= fun () ->
                to_channel oc bool a.finc >>= fun () ->
                to_channel oc (option string) a.last >>= fun () ->
                to_channel oc bool a.linc >>= fun () ->
                to_channel oc int a.max
            in
            make ~from_channel ~to_channel

        let reversed_array at =
            let from_channel ic =
                from_channel ic int >>= fun len ->
                let rec loop acc = function
                  | 0 -> Lwt.return acc
                  | n -> begin
                      from_channel ic at >>= fun a ->
                      loop (a :: acc) (n - 1)
                  end
                in
                loop [] len >>= fun l ->
                let a = Array.of_list l in
                Lwt.return (ReversedArray.make a)
            and to_channel oc a =
                let a' = ReversedArray.array a in
                let l = Array.length a' in
                to_channel oc int l >>= fun () ->
                let rec loop = function
                  | 0 -> Lwt.return ()
                  | n -> begin
                      let v = Array.get a' (n - 1) in
                      to_channel oc at v >>= fun () ->
                      loop (n - 1)
                  end
                in
                loop l
            in
            make ~from_channel ~to_channel

        let key =
            let from_channel ic =
                from_channel ic string >>= fun s ->
                Lwt.return (Key.make (Simple_store.__prefix ^ s))
            and to_channel = Llio.output_key in
            make ~from_channel ~to_channel

        let counted_list at =
            let from_channel ic =
                from_channel ic (list at) >>= fun l ->
                Lwt.return (CountedList.of_list l)
            and to_channel oc l =
                let count = CountedList.length l
                and list = CountedList.list l in
                Llio.output_counted_list (fun oc v -> to_channel oc at v) oc (count, list)
            in
            make ~from_channel ~to_channel

        let update =
            let from_channel ic =
                from_channel ic string >>= fun data ->
                let buf = Llio.make_buffer data 0 in
                let update = Update.Update.from_buffer buf in
                Lwt.return update
            and to_channel oc u =
                let buf = Buffer.create 64 in
                let () = Update.Update.to_buffer buf u in
                to_channel oc string (Buffer.contents buf)
            in
            make ~from_channel ~to_channel

        let statistics =
            let from_channel ic =
                from_channel ic string >>= fun s ->
                let buf = Llio.make_buffer s 0 in
                let stats = Statistics.Statistics.from_buffer buf in
                Lwt.return stats
            and to_channel oc s =
                let buf = Buffer.create 100 in
                let () = Statistics.Statistics.to_buffer buf s in
                to_channel oc string (Buffer.contents buf)
            in
            make ~from_channel ~to_channel
    end

    type ('req, 'res) t =
      | Ping : ((string * string), string Result.t) t
      | Who_master : (unit, string option Result.t) t
      | Exists : ((Arakoon_client.consistency * string), bool Result.t) t
      | Get : ((Arakoon_client.consistency * string), string Result.t) t
      | Set : ((string * string), unit Result.t) t
      | Delete : (string, unit Result.t) t
      | Range : ((Arakoon_client.consistency * RangeRequest.t), Key.t ReversedArray.t Result.t) t
      | Prefix_keys : ((Arakoon_client.consistency * string * int), Key.t CountedList.t Result.t) t
      | Test_and_set : ((string * string option * string option), string option Result.t) t
      | Range_entries : ((Arakoon_client.consistency * RangeRequest.t), (Key.t * string) CountedList.t Result.t) t
      | Sequence : (Update.Update.t, unit Result.t) t
      | Multi_get : ((Arakoon_client.consistency * string CountedList.t), string list Result.t) t
      | Expect_progress_possible : (unit, bool Result.t) t
      | Statistics : (unit, Statistics.Statistics.t Result.t) t
      | User_function : ((string * string option), string option Result.t) t
      | Assert : ((Arakoon_client.consistency * string * string option), unit Result.t) t
      | Get_key_count : (unit, int64 Result.t) t
      | Confirm : ((string * string), unit Result.t) t
      | Rev_range_entries : ((Arakoon_client.consistency * RangeRequest.t), (Key.t * string) CountedList.t Result.t) t
      | Synced_sequence : (Update.Update.t, unit Result.t) t
      | Optimize_db : (unit, unit Result.t) t
      | Defrag_db : (unit, unit Result.t) t
      | Delete_prefix : (string, int Result.t) t
      | Version : (unit, (int * int * int * string) Result.t) t
      | Assert_exists : ((Arakoon_client.consistency * string), unit Result.t) t
      | Drop_master : (unit, unit Result.t) t
      | Multi_get_option : ((Arakoon_client.consistency * string CountedList.t), string option list Result.t) t
      | Current_state : (unit, string Result.t) t
      | Replace : ((string * string option), string option Result.t) t
      | Nop : (unit, unit Result.t) t
      | Flush_store : (unit, unit Result.t) t
      | Get_txid : (unit, Arakoon_client.consistency Result.t) t
      | Copy_db_to_head : (int, unit Result.t) t

    let meta : type r s. (r, s) t -> (r Type.t * s Type.t) =
        let open Type in
        function
          | Ping -> tuple2 string string, result string
          | Who_master -> unit, result (option string)
          | Exists -> tuple2 consistency string, result bool
          | Get -> tuple2 consistency string, result string
          | Set -> tuple2 string string, result unit
          | Delete -> string, result unit
          | Range -> tuple2 consistency range_request, result (reversed_array key)
          | Prefix_keys -> tuple3 consistency string int, result (counted_list key)
          | Test_and_set -> tuple3 string (option string) (option string), result (option string)
          | Range_entries -> tuple2 consistency range_request, result (counted_list (tuple2 key string))
          | Sequence -> update, result unit
          | Multi_get -> tuple2 consistency (counted_list string), result (list string)
          | Expect_progress_possible -> unit, result bool
          | Statistics -> unit, result statistics
          | User_function -> tuple2 string (option string), result (option string)
          | Assert -> tuple3 consistency string (option string), result unit
          | Get_key_count -> unit, result int64
          | Confirm -> tuple2 string string, result unit
          | Rev_range_entries -> tuple2 consistency range_request, result (counted_list (tuple2 key string))
          | Synced_sequence -> update, result unit
          | Optimize_db -> unit, result unit
          | Defrag_db -> unit, result unit
          | Delete_prefix -> string, result int
          | Version -> unit, result (tuple4 int int int string)
          | Assert_exists -> tuple2 consistency string, result unit
          | Drop_master -> unit, result unit
          | Multi_get_option -> tuple2 consistency (counted_list string), result (list (option string))
          | Current_state -> unit, result string
          | Replace -> tuple2 string (option string), result (option string)
          | Nop -> unit, result unit
          | Flush_store -> unit, result unit
          | Get_txid -> unit, result consistency
          | Copy_db_to_head -> int, result unit

    type some_t = Some_t : (_, _) t -> some_t

    module Tag = struct
        let mAGIC = Common._MAGIC
        let mASK = Common._MASK

        type t = Int32.t

        let compare = compare

        exception Invalid_magic of t

        let from_channel ic =
            Llio.input_int32 ic >>= fun masked ->
            let magic = Int32.logand masked mAGIC in
            if magic <> mAGIC
            then
                Lwt.fail (Invalid_magic magic)
            else
                Lwt.return (Int32.logand masked mASK)

        let to_channel oc t =
            let masked = Int32.logor t mAGIC in
            Llio.output_int32 oc masked
    end

    let command_map = [ Some_t Ping, 0x01l
                      ; Some_t Who_master, 0x02l
                      ; Some_t Exists, 0x07l
                      ; Some_t Get, 0x08l
                      ; Some_t Set, 0x09l
                      ; Some_t Delete, 0x0al
                      ; Some_t Range, 0x0bl
                      ; Some_t Prefix_keys, 0x0cl
                      ; Some_t Test_and_set, 0x0dl
                      ; Some_t Range_entries, 0x0fl
                      ; Some_t Sequence, 0x10l
                      ; Some_t Multi_get, 0x11l
                      ; Some_t Expect_progress_possible, 0x12l
                      ; Some_t Statistics, 0x13l
                      ; Some_t User_function, 0x15l
                      ; Some_t Assert, 0x16l
                      ; Some_t Get_key_count, 0x1al
                      ; Some_t Confirm, 0x1cl
                      ; Some_t Rev_range_entries, 0x23l
                      ; Some_t Synced_sequence, 0x24l
                      ; Some_t Optimize_db, 0x25l
                      ; Some_t Defrag_db, 0x26l
                      ; Some_t Delete_prefix, 0x27l
                      ; Some_t Version, 0x28l
                      ; Some_t Assert_exists, 0x29l
                      ; Some_t Drop_master, 0x30l
                      ; Some_t Multi_get_option, 0x31l
                      ; Some_t Current_state, 0x32l
                      ; Some_t Replace, 0x33l
                      ; Some_t Nop, 0x41l
                      ; Some_t Flush_store, 0x42l
                      ; Some_t Get_txid, 0x43l
                      ; Some_t Copy_db_to_head, 0x44l
                      ]
end

module Client = Rpc.Client(Protocol)
