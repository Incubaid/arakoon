open Core
open Lwt

module UserDB = (struct
  type tx = BS.tx
  let set tx k v = 
    Lwtc.log "UserDB.set %S %S" k v>>= fun () ->
    Lwt.return ()

  let get tx k = 
    Lwtc.log "UserDB.get %S" k >>= fun () ->
    Lwt.return None
    
end : sig
  type tx = BS.tx
  val set : tx -> k -> v -> unit Lwt.t
  val get : tx -> k -> v option Lwt.t
end)

module Registry = struct
  type f = UserDB.tx -> string option -> (string option) Lwt.t
  let _r = Hashtbl.create 42
  let register (name:string) (f:f) = Hashtbl.replace _r name f
  let lookup (name:string) = Hashtbl.find _r name
end
