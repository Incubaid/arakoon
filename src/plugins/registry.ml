class type user_db =
  object
    method set : string -> string -> unit
    method get : string -> string
    method delete: string -> unit
    method test_and_set: string -> string option -> string option -> string option
    method range_entries: string option -> bool -> string option -> bool -> int
      -> (string * string) list
  end

module Registry = struct
  type f = user_db -> string option -> string option
  let _r = Hashtbl.create 42
  let register name (f:f) = Hashtbl.replace _r name f
  let lookup name = Hashtbl.find _r name
end
