open Otc


module type Dict = sig
  val set : key:string -> value:string -> unit
  val get : key:string -> string
end 

module Registry : sig
  type f = Bdb.bdb -> string option -> string option
  val register : string -> f -> unit
  val lookup : string -> f
end = struct
  type f = Bdb.bdb -> string option -> string option
  let _r = Hashtbl.create 42
  let register name (f:f) = Hashtbl.replace _r name f
  let lookup name = Hashtbl.find _r name
end
