open Core
open Lwt


module UserDB = struct
  type tx = BS.tx

  type v = string
  type k = string

  let set tx k v = 
    Lwtc.log "UserDB.set %S %S" k v>>= fun () ->
    let k' = pref_key k in
    BS.set tx k' v >>= fun () ->
    Lwt.return ()

  let get tx k = 
    Lwtc.log "UserDB.get %S" k >>= fun () ->
    let k' = pref_key k in
    BS.get tx k'

  let delete tx k = 
    Lwtc.log "UserDB.delete %S" k >>= fun ()->
    let k' = pref_key k in
    BS.delete tx k'
    
end 

module Registry = struct
  type f = UserDB.tx -> string option -> (string option) Lwt.t
  type f2 = (Core.k, Core.v) Hashtbl.t -> string option -> (string option) Lwt.t
  (*type f2 = (string, string) Hashtbl.t -> string option -> (string option) Lwt.t*)
  let _r = Hashtbl.create 42
  let _r2 = Hashtbl.create 42
  let register (name:string) (f:f) = Hashtbl.replace _r name f
  let register2 (name:string) (f2:f2) = Hashtbl.replace _r2 name f2
  let lookup (name:string) = Hashtbl.find _r name
  let lookup2 (name:string) = Hashtbl.find _r2 name
end
