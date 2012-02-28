open Core

module MemStore = (struct
  type t = (k, v) Hashtbl.t

  let write t u = 
    begin
      match u with
        | SET (k,v) -> Hashtbl.replace t k v
        | DELETE k  -> Hashtbl.remove  t k
    end;
    Lwt.return ()
        

  let create () = Hashtbl.create 7

  let get t k = let v = Hashtbl.find t k in Lwt.return v

end: STORE)

