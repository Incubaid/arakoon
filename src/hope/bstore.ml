open Core
module BStore = (struct
  type t = { m: Lwt_mutex.t; store: Baardskeerder.t}

  let create fn = 
    let () = Baardskeerder.init fn in
    {m = Lwt_mutex.create();
     store = Baardskeerder.make fn;
    }
    
  let commit t i = 
    Lwt.return Core.UNIT
    
  let log t i u = 
    let _inner tx = 
      match u with
        | SET (k,v) -> Baardskeerder.set tx k v
        | DELETE k  -> Baardskeerder.delete tx k
    in
    Lwt_mutex.with_lock t.m 
      (fun () -> 
        let () = Baardskeerder.with_tx t.store _inner in Lwt.return Core.UNIT)

    
  let get t k = 
    let _inner () = 
      let v = Baardskeerder.get_latest t.store k in
      Lwt.return v
    in
    Lwt_mutex.with_lock t.m _inner

end: STORE)
