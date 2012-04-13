open Core
open Lwt
open Baardskeerder 

module BS = Baardskeerder(Logs.Flog0)(Stores.Lwt) 

module BStore = (struct
  type t = { m: Lwt_mutex.t; store: BS.t; mutable meta: string option}

  let init fn = 
    BS.init fn 
    
  let create fn = 
    BS.make fn >>= fun s ->
    Lwt.return 
    {
      m = Lwt_mutex.create();
      store = s;
      meta = None;
    }
  
  let pref_key k = "@" ^ k
  
  let commit t i = 
    Lwt_mutex.with_lock t.m
    ( fun() ->
      BS.commit_last t.store >>= fun () -> 
      Lwt.return Core.UNIT 
    )
    
    
  let log t d u =
    let _exec tx =
      begin 
        let rec _inner tx = function
          | SET (k,v) -> BS.set tx (pref_key k) v
          | DELETE k  -> BS.delete tx (pref_key k)
          | SEQUENCE s -> Lwt_list.iter_s (fun u -> _inner tx u) s
        in _inner tx u
      end
    in  
    Lwt_mutex.with_lock t.m 
      (fun () -> 
        BS.log_update t.store ~diff:d _exec >>= fun () ->
        Lwt.return Core.UNIT)
  
  let last_update t =
    let convert_update = function
      | Set (k,v) -> Core.SET(k,v)
      | Delete k -> Core.DELETE k
    in
    BS.last_update t.store >>= fun m_last ->
    begin
      match m_last with
        | None -> Lwt.return None
        | Some (i_time, ups) ->
          begin
            match ups with
              | [] ->
                failwith "No update logged???" 
              | u :: [] ->
                Lwt.return (Some (i_time, convert_update u))
              | _ ->
                let convs = List.fold_left ( fun acc u -> (convert_update) u :: acc ) [] ups in
                Lwt.return (Some (i_time, Core.SEQUENCE convs))
          end
          
    end
      
  let get t k = 
    BS.get_latest t.store (pref_key k) 
  
  let get_meta t =
    match t.meta with
      | None -> BS.get_metadata t.store
      | Some m -> Lwt.return (Some m)

  let set_meta t s =
    t.meta <- Some s;
    BS.set_metadata t.store s
end)

