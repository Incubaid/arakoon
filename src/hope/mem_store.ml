open Core
open Lwt
open Routing

module MemStore = (struct
  type t = { 
    store: (k, v) Hashtbl.t; 
    mutable meta: string option; 
    read_only:bool
  }

  let rec log t i u = 
    begin
      let _ok = Lwt.return (Baardskeerder.OK None) in
      match u with
        | SET (k,v) -> let () = Hashtbl.replace t.store k v in _ok
        | DELETE k  -> 
            if Hashtbl.mem t.store k 
            then let () = Hashtbl.remove  t.store k in _ok
            else Lwt.return (Baardskeerder.NOK (Arakoon_exc.E_NOT_FOUND,k))
        | SEQUENCE s -> 
            let rec _loop = function
              | [] -> _ok
              | u :: us -> 
                  begin
                    log t i u >>= function
                      | Baardskeerder.OK _ -> _loop us
                      | x -> Lwt.return x
                  end
            in
            _loop s
    end
  
  let is_read_only t = t.read_only
  
  let commit t i = Lwt.return ()

  let create n ro = Lwt.return { store = Hashtbl.create 7 ; meta = None ; read_only = ro}

  let get t k = 
    try
      let v = Hashtbl.find t.store k in 
      Lwt.return (Some v)
    with Not_found -> Lwt.return None
  
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

  let get_fringe t (boundary:string option) dir = 
    let ok = 
      match boundary with
        | None -> fun _ -> true
        | Some boundary ->
            match dir with 
              | Routing.LOWER_BOUND ->
                  fun k -> k > boundary
              | Routing.UPPER_BOUND ->
                  fun k -> k < boundary
    in
    let kvs = 
      Hashtbl.fold 
        (fun k v acc -> 
          if ok k
          then (k,v) :: acc
          else acc) t.store []
    in
    Lwt.return kvs
end: STORE)

