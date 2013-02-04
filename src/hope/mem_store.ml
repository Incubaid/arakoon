open Core
open Lwt

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
        | USER_FUNCTION (name,po) -> 
            let f = Userdb.Registry.lookup2 name in
            f t.store po >>= fun ro -> 
            Lwtc.log "Mstore.returning %s" (Log_extra.string_option_to_string ro) >>= fun () ->
            let a = match ro with 
              | None   -> (Baardskeerder.OK None)
              | Some v -> (Baardskeerder.NOK (Arakoon_exc.E_NOT_FOUND,v))
            in
            Lwt.return a
        | DELETE k  -> 
            if Hashtbl.mem t.store k 
            then let () = Hashtbl.remove  t.store k in _ok
            else Lwt.return (Baardskeerder.NOK (Arakoon_exc.E_NOT_FOUND,k))
        | ASSERT (k,v) ->
            let _nok =  Lwt.return (Baardskeerder.NOK (Arakoon_exc.E_ASSERTION_FAILED,k)) in
            if Hashtbl.mem t.store k then 
              begin
                if (Some (Hashtbl.find t.store k)) = v then _ok
                else _nok
              end
            else _nok
        | ASSERT_EXISTS (k) ->
            let _nok =  Lwt.return (Baardskeerder.NOK (Arakoon_exc.E_ASSERTION_FAILED,k)) in
            if Hashtbl.mem t.store k 
            then _ok
            else Lwt.return (Baardskeerder.NOK (Arakoon_exc.E_ASSERTION_FAILED,k))

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

end: STORE)

