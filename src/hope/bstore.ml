open Core
open Lwt
open Baardskeerder 

module BS = Baardskeerder(Logs.Flog0)(Stores.Lwt) 




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
  let unpref_key k = String.sub k 1 ((String.length k) -1)
  
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
      | Set (k,v) -> Core.SET(unpref_key k,v)
      | Delete k -> Core.DELETE (unpref_key k)
    in
    BS.last_update t.store >>= fun m_last ->
    begin
      match m_last with
        | None -> Lwt.return None
        | Some (i_time, ups, committed) ->
          begin
            let tick_i = TICK i_time in
            let cvo = 
              if committed then None
              else
                match ups with
                  | []      -> failwith "No update logged???" 
                  | u :: [] -> Some (convert_update u)
                  | _       -> let convs = List.fold_left ( fun acc u -> (convert_update) u :: acc ) [] ups in
                               Some (Core.SEQUENCE convs)
            in 
            Lwt.return (Some (tick_i, cvo)) 
          end
          
    end
      
  let get t k = BS.get_latest t.store (pref_key k) 

  let range t first finc last linc max = 
    let px = function
      | None -> None
      | Some k -> Some (pref_key k)
    in
    let mo = 
      if max = -1 then None
      else Some max 
    in
    BS.range_latest t.store 
      (px first) finc
      (px last) linc
      mo
    >>= fun ks ->
    Lwt.return (List.map unpref_key ks)

  let last_entries t (t0:Core.tick) (oc:Llio.lwtoc) = 
    let TICK i0 = t0 in
    let f acc i actions = 
      Lwtc.log "f ... %Li ..." i >>= fun () ->
      Llio.output_int64 oc i >>= fun () ->
      Llio.output_list output_action oc actions >>= fun () ->
      Lwt.return acc 
    in
    let a0 = () in
    Lwt_io.printlf "Bstore.last_entries %Li" i0 >>= fun () ->
    Lwt_mutex.with_lock t.m (fun () -> BS.catchup t.store i0 f a0) >>= fun a ->
    Lwt_io.printlf "done">>= fun () ->
    Lwt.return ()

end)

