open Core
open Lwt

module MemStore = (struct
  type t = (k, v) Hashtbl.t

  let log t i u = 
    begin
      match u with
        | SET (k,v) -> Hashtbl.replace t k v
        | DELETE k  -> Hashtbl.remove  t k
    end;
    Lwt.return Core.UNIT
  
  let commit t i = Lwt.return Core.UNIT

  let create n = Hashtbl.create 7

  let get t k = let v = Hashtbl.find t k in Lwt.return v

end: STORE)

