
module UserDB : 
sig
  type tx = Core.BS.tx
  type k = string
  type v = string
  val set    : tx -> k -> v -> unit Lwt.t
  val get    : tx -> k -> (v, k) Baardskeerder.result Lwt.t
  val delete : tx -> k -> (unit, Baardskeerder.k) Baardskeerder.result Lwt.t
end

module Registry:
sig
  type f = UserDB.tx -> string option -> (string option) Lwt.t
  type f2 = (Core.k,Core.v) Hashtbl.t -> string option -> (string option) Lwt.t
  val register2: string -> f2 ->unit
  val register: string -> f -> unit
  val lookup: string -> f
  val lookup2: string -> f2
end
    
