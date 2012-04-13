open Core
open Lwt
open Baardskeerder

module BS = Baardskeerder(Logs.Flog0)(Stores.Lwt) 

module BStore = (struct
  type t = { m: Lwt_mutex.t; store: BS.t}

  let init fn = 
    BS.init fn 
    
  let create fn = 
    BS.make fn >>= fun s ->
    Lwt.return 
    {
      m = Lwt_mutex.create();
      store = s;
    }
    
  let commit t i = 
    Lwt.return Core.UNIT
    
  let log t i u = 
    let _inner tx = 
      match u with
        | SET (k,v) -> BS.set tx k v
        | DELETE k  -> BS.delete tx k
    in
    Lwt_mutex.with_lock t.m 
      (fun () -> 
        BS.with_tx t.store _inner >>= fun () ->
        Lwt.return Core.UNIT)
    
  let get t k = 
    BS.get_latest t.store k 

end)
