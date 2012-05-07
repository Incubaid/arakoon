open Core
open Lwt
open Baardskeerder 

module BS = Baardskeerder(Logs.Flog0)(Stores.Lwt) 

let __prefix = "@"
let __admin_prefix = "*"

let pref_key ?(_pf = __prefix) k = _pf ^ k
let unpref_key ?(_pf = __prefix) k = 
  let to_cut = String.length _pf in
  String.sub k to_cut ((String.length k) - to_cut)
    
let action2update = function
  | Set (k,v) -> Core.SET(unpref_key k,v)
  | Delete k -> Core.DELETE (unpref_key k)
    

module BStore = (struct
  type t = { m: Lwt_mutex.t; store: BS.t; mutable meta: string option}

  type tx_result =
  | TX_SUCCESS
  | TX_ERROR of k


  let init fn = 
    BS.init fn 
    
  let create fn = 
    BS.make fn >>= fun store ->
    let m = Lwt_mutex.create() in
    let r = {m=m; store=store; meta=None;} in
    Lwt.return r
  
  let set_meta t s =
    Lwt.return (t.meta <- Some s)
  
  let get_meta t =
    Lwt.return t.meta
    
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
        let rec _inner (tx: BS.tx) = function
          | Core.SET (k,v) -> BS.set tx (pref_key k) v >>= fun () -> Lwt.return (OK ())
          | Core.DELETE k  -> BS.delete tx (pref_key k)
          | Core.ADMIN_SET (k, m_v) -> 
            begin
              let k' = pref_key ~_pf:__admin_prefix k in
              match m_v with
              | None -> BS.delete tx k'
              | Some v -> BS.set tx k' v >>= fun () -> Lwt.return (OK ())
            end
          | Core.ASSERT (k, m_v) -> 
            begin
              BS.get tx k >>= function
              | OK v' -> if m_v <> (Some v') then (Lwt.return (NOK k)) else (Lwt.return (OK ())) 
              | NOK k -> if m_v <> None then (Lwt.return (NOK k)) else (Lwt.return (OK ())) 
            end
          | Core.SEQUENCE s -> 
            Lwt_list.fold_left_s 
            (fun a u ->
              match a with
                | OK _ -> _inner tx u 
                | NOK k -> Lwt.return (NOK k) )
            (OK ())
            s
        in _inner tx u
      end
    in  
    Lwt_mutex.with_lock t.m 
      (fun () -> 
        BS.log_update t.store ~diff:d _exec >>= function 
          | OK _ -> Lwt.return TX_SUCCESS
          | NOK k -> Lwt.return (TX_ERROR k)
      )
  
  let last_update t =
    BS.last_update t.store >>= fun m_last ->
    begin
      match m_last with
        | None -> Lwt.return None
        | Some (i_time, ups, committed) ->
          begin
            let tick_i = TICK i_time in
            let cvo = 
              if committed 
              then None
              else
                match ups with
                  | []      -> failwith "No update logged???" 
                  | a :: [] -> Some (action2update a)
                  | _       -> let convs = List.fold_left ( fun acc a -> action2update a :: acc ) [] ups in
                               Some (Core.SEQUENCE convs)
            in 
            Lwt.return (Some (tick_i, cvo)) 
          end
          
    end
      
  let get t k = 
    BS.get_latest t.store (pref_key k) >>= function
      | OK v -> Lwt.return (Some v)
      | NOK k -> Lwt.return None 

  let opx first= function
    | None when first -> Some (pref_key "")
    | None -> None
    | Some k -> Some (pref_key k)

  let range t first finc last linc max = 
    BS.range_latest t.store 
      (opx true first ) finc
      (opx false last) linc
      max
    >>= fun ks ->
    Lwt.return (List.map unpref_key ks)

      
  let prefix_keys t prefix max = 
    let prefix' = pref_key prefix in
    BS.prefix_keys_latest t.store prefix' max >>= fun keys ->
    Lwt.return (List.map unpref_key keys)
    
  let last_entries t (t0:Core.tick) (oc:Llio.lwtoc) = 
    let TICK i0 = t0 in
    let f acc i actions = 
      Lwtc.log "f ... %Li ..." i >>= fun () ->
      Llio.output_int64 oc i >>= fun () ->
      Llio.output_list output_action oc actions >>= fun () ->
      Lwt.return acc 
    in
    let a0 = () in
    Lwtc.log "Bstore.last_entries %Li" i0 >>= fun () ->
    Lwt_mutex.with_lock t.m (fun () -> BS.catchup t.store i0 f a0) >>= fun a ->
    Lwtc.log "Bstore.last_entries done">>= fun () ->
    Lwt.return ()

  let _do_range_entries inner t first finc last linc max =
    inner t.store (opx true first) finc (opx false last) linc max
    >>= fun kvs ->
    let unpref_kv (k,v) = (unpref_key k, v) in
    Lwt.return (List.map unpref_kv kvs)
    
  let range_entries t first finc last linc max =
    _do_range_entries BS.range_entries_latest t first finc last linc max

  let rev_range_entries t first finc last linc max =
    _do_range_entries BS.rev_range_entries_latest t first finc last linc max
    
  let dump t =
    Lwtc.failfmt "todo"
    
end: STORE)

