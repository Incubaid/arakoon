open Core
open Lwt

module MemStore = (struct
  type t = { store: (k, v) Hashtbl.t; mutable meta: string option; read_only:bool}
  type tx_result = 
  | TX_SUCCESS of v option
  | TX_NOT_FOUND of k
  | TX_ASSERT_FAIL of k
  
  let rec log t i u = 
    begin
      match u with
        | SET (k,v) -> Lwt.return (Hashtbl.replace t.store k v)
        | DELETE k  -> Lwt.return (Hashtbl.remove  t.store k)
        | SEQUENCE s -> Lwt_list.iter_s (fun u -> log t i u >>= fun _ -> Lwt.return ()) s
    end
    >>= fun () -> 
    Lwt.return (TX_SUCCESS None)
  
  let is_read_only t = t.read_only
  
  let commit t i = Lwt.return ()

  let create n ro = Lwt.return { store = Hashtbl.create 7 ; meta = None ; read_only = ro}

  let get t k = let v = Hashtbl.find t.store k in Lwt.return (Some v)
  
  let admin_get = get
  
  let set_meta t s = 
    t.meta <- Some s;
    Lwt.return ()
    
  let get_meta t =
    Lwt.return t.meta 
  
  let last_entries t i oc = Lwtc.failfmt "todo"
    
  let range t first finc last linc max = Lwtc.failfmt "todo"
  let range_entries t first finc last linc max = Lwtc.failfmt "todo"
  let rev_range_entries t first finc last linc max = Lwtc.failfmt "todo"
  let prefix_keys t prefix max = Lwtc.failfmt "todo"
  let admin_prefix_keys t prefix = prefix_keys t prefix None
  
  let last_update t = Lwtc.failfmt "todo"
  
  let close t = Lwt.return ()
  let init fn = Lwt.return ()

  let dump t = Lwtc.failfmt "todo"

  let raw_dump t oc = Lwtc.failfmt "todo: MemStore.raw_dump"
  let get_key_count t = Lwtc.failfmt "todo: MemStore.get_key_count"
end: STORE)

