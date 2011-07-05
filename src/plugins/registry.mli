class type user_db = 
object 
  method set : string -> string -> unit
  method get : string -> string
  method delete: string -> unit
end


module Registry : sig
  type f = user_db -> string option -> string option
  val register : string -> f -> unit
  val lookup : string -> f
end 
