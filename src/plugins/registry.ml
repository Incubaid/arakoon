
class type user_db = 
object 
  method put : string -> string -> unit
  method get : string -> string
end

module Registry : sig
  type f = user_db -> string option -> string option
  val register : string -> f -> unit
  val lookup : string -> f
end = struct
  type f = user_db -> string option -> string option
  let _r = Hashtbl.create 42
  let register name (f:f) = Hashtbl.replace _r name f
  let lookup name = Hashtbl.find _r name
end
