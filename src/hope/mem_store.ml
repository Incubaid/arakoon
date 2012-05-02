open Core
open Lwt

module MemStore = (struct
  type t = { store: (k, v) Hashtbl.t; mutable meta: string option}

  let rec log t i u = 
    begin
      match u with
        | SET (k,v) -> Lwt.return (Hashtbl.replace t.store k v)
        | DELETE k  -> Lwt.return (Hashtbl.remove  t.store k)
        | SEQUENCE s -> Lwt_list.iter_s (fun u -> log t i u >>= fun _ -> Lwt.return ()) s
    end
    >>= fun () -> 
    Lwt.return Core.UNIT
  
  let commit t i = Lwt.return Core.UNIT

  let create n = Lwt.return { store = Hashtbl.create 7 ; meta = None }

  let get t k = let v = Hashtbl.find t.store k in Lwt.return (Some v)
  
  let set_meta t s = 
    t.meta <- Some s;
    Lwt.return ()
    
  let get_meta t =
    Lwt.return t.meta 
  
  let last_entries t i oc = Lwtc.failfmt "todo"
    
  let range t first finc last linc max = Lwtc.failfmt "todo"
  let range_entries t first finc last linc max = Lwtc.failfmt "todo"
  let rev_range_entries t first finc last linc max = Lwtc.failfmt "todo"
  
  let last_update t = Lwtc.failfmt "todo"
  
  let close t = Lwt.return ()
  let init fn = Lwt.return ()

  let dump t = Lwtc.failfmt "todo"
end: STORE)

