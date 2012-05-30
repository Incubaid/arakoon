
module UserDB : 
sig
  type tx = Core.BS.tx
  type k = string
  type v = string
  val set : tx -> k -> v -> unit Lwt.t
  val get : tx -> k -> Baardskeerder.v Baardskeerder.result Lwt.t
end

module Registry:
sig
  type f = UserDB.tx -> string option -> (string option) Lwt.t
  val register: string -> f -> unit
  val lookup: string -> f
end
    
