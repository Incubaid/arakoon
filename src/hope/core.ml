module type SM = sig
  type state
  type action
  type msg

  val state2s : state -> string
  val action2s : action -> string
  val msg2s : msg -> string

  val start : state
  val step: msg -> state -> action * state
end

type k = string
type v = string

type update = 
  | SET of k * v
  | DELETE of k

let update2s = function
  | SET (k,v) -> Printf.sprintf "U_SET (k: %s)" k
  | DELETE k -> Printf.sprintf "U_DEL (k: %s)" k

type result = 
  | UNIT
  | FAILURE of Arakoon_exc.rc * string

module type STORE = sig
  type t
  
  val write : t -> update -> unit Lwt.t
  val get : t -> k -> v Lwt.t
  val create : unit -> t
end
