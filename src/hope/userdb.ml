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
    
end 

module Registry = struct
  type f = UserDB.tx -> string option -> (string option) Lwt.t
  let _r = Hashtbl.create 42
  let register (name:string) (f:f) = Hashtbl.replace _r name f
  let lookup (name:string) = Hashtbl.find _r name
end
