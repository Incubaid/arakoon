class type user_db =
  object
    method set : string -> string -> unit
    method get : string -> string
    method delete: string -> unit
    method test_and_set: string -> string option -> string option -> string option
    method range_entries: string option -> bool -> string option -> bool -> int
      -> (string * string) list
  end


module Registry : sig
  type f = user_db -> string option -> string option
  val register : string -> f -> unit
  val lookup : string -> f
end
