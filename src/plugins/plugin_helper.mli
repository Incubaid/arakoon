(* ... *)

val serialize_string  : Buffer.t -> string -> unit
val serialize_hashtbl : Buffer.t -> (Buffer.t -> 'a -> 'b -> unit) ->
                        ('a, 'b) Hashtbl.t -> unit

val serialize_string_list: Buffer.t -> string list -> unit

type input
val make_input : string -> int -> input

val deserialize_string : input -> string
val deserialize_hashtbl: input -> (input -> 'a * 'b) -> ('a, 'b) Hashtbl.t
val deserialize_string_list: input -> string list

val debug_f: ('a, unit, string, unit) format4 -> 'a
