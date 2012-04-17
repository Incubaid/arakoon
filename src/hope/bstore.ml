open Core
open Lwt
open Baardskeerder 

module BS = Baardskeerder(Logs.Flog0)(Stores.Lwt) 


let output_action (oc:Llio.lwtoc) (action:action) = 
  match action with
    | Set (k,v) -> Lwt.return () 
    | Delete k  -> Lwt.return ()

module BStore = (struct
  type t = { m: Lwt_mutex.t; store: BS.t;}

  let init fn = 
    BS.init fn 
    
  let create fn = 
    BS.make fn >>= fun s ->
    Lwt.return 
    {
      m = Lwt_mutex.create();
      store = s;
    }
  
  let pref_key k = "@" ^ k
  
  let commit t i = 
    Lwt_mutex.with_lock t.m
    ( fun() ->
      BS.commit_last t.store >>= fun () -> 
      Lwt.return Core.UNIT 
    )
    
  let close t = BS.close t.store
  
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
        | Some (i_time, ups, committed) ->
          begin
            let i_time = TICK i_time in
            match ups with
              | [] ->
                failwith "No update logged???" 
              | u :: [] ->
                if committed
                then
                  Lwt.return (Some (i_time, None))
                else 
                  Lwt.return (Some (i_time, Some (convert_update u)))
              | _ ->
                if committed
                then
                  Lwt.return (Some (i_time, None))
                else
                  let convs = List.fold_left ( fun acc u -> (convert_update) u :: acc ) [] ups in
                  Lwt.return (Some (i_time, Some (Core.SEQUENCE convs)))
          end
          
    end
      
  let get t k = BS.get_latest t.store (pref_key k) 

  let last_entries t (t0:Core.tick) (oc:Llio.lwtoc) = 
    let TICK i0 = t0 in
    let f acc i actions = 
      Llio.output_int64 oc i >>= fun () ->
      Llio.output_list output_action oc actions >>= fun () ->
      Lwt.return acc 
    in
    let a0 = () in
    Lwt_mutex.with_lock t.m (fun () -> BS.catchup t.store i0 f a0) >>= fun a ->
    Lwt.return ()

end)

