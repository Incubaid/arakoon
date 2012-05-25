open Core
open Lwt
open Baardskeerder 
open Unix


    
let action2update = function
  | Set (k,v) -> Core.SET(unpref_key k,v)
  | Delete k -> Core.DELETE (unpref_key k)
    

module BStore = (struct
  type t = { m: Lwt_mutex.t; 
             store: BS.t; 
             mutable meta: string option; 
             read_only: bool;
             fn : string;
           }

  type tx_result =
  | TX_SUCCESS of v option
  | TX_NOT_FOUND of k
  | TX_ASSERT_FAIL of k

  type seq_result = 
  | SEQ_SUCCESS of v option
  | SEQ_ASSERT_FAIL of k

  let init fn = 
    BS.init fn 
    
  let create fn read_only = 
    BS.make fn >>= fun store ->
    let m = Lwt_mutex.create() in
    let r = {
      m; 
      store; 
      meta=None; 
      read_only; 
      fn
    } 
    in
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
        let ok_none  = (OK (SEQ_SUCCESS None)) in
        let rec _inner (tx: BS.tx) = function
          | Core.SET (k,v) ->
            Lwtc.log "set: %s" k >>= fun () -> 
            BS.set tx (pref_key k) v >>= fun () -> Lwt.return ok_none
          | Core.DELETE k  -> 
            begin
              Lwtc.log "del: %s" k >>= fun () ->
              BS.delete tx (pref_key k) >>= function
                | NOK k -> Lwt.return (NOK (unpref_key k))
                | a -> Lwt.return ok_none
            end
          | Core.ADMIN_SET (k, m_v) -> 
            begin
              let k' = pref_key ~_pf:__admin_prefix k in
              match m_v with
              | None -> BS.delete tx k' >>= fun _ -> Lwt.return ok_none
              | Some v -> BS.set tx k' v >>= fun () -> Lwt.return ok_none
            end
          | Core.ASSERT (k, m_v) -> 
            begin
              BS.get tx (pref_key k) >>= fun r ->              
              let txr = match r with
                | OK v' -> 
                  if m_v <> (Some v') 
                  then SEQ_ASSERT_FAIL k
                  else (SEQ_SUCCESS None)
                | NOK k -> 
                  if m_v <> None 
                  then SEQ_ASSERT_FAIL k
                  else (SEQ_SUCCESS None)
              in
              Lwt.return (OK txr)
            end
          | Core.SEQUENCE s -> 
            Lwt_list.fold_left_s 
            (fun a u ->
              match a with
                | OK (SEQ_SUCCESS None) -> _inner tx u
                | NOK k -> Lwt.return (NOK k) 
                | a -> Lwt.return a
            )
            ok_none
              s 
          | Core.USER_FUNCTION (name, po) ->
            let f = Userdb.Registry.lookup name in
            f tx po >>= fun ro -> Lwtc.failfmt "USER_FUNCTION: got so here"

        in _inner tx u
      end
    in  
    Lwt_mutex.with_lock t.m 
      (fun () -> 
        BS.log_update t.store ~diff:d _exec >>= fun r ->
        let rr = match r with
          | OK SEQ_SUCCESS x-> (TX_SUCCESS x)
          | OK (SEQ_ASSERT_FAIL k) -> (TX_ASSERT_FAIL k)
          | NOK k -> (TX_NOT_FOUND k)
        in
        Lwt.return rr
      )
  let is_read_only t = t.read_only
  
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
  
  let admin_get t k =
    BS.get_latest t.store (pref_key ~_pf:__admin_prefix k) >>= function 
      | OK v -> Lwt.return (Some v)
      | NOK k -> Lwt.return None
     
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
    BS.catchup t.store i0 f a0 >>= fun a ->
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

  let raw_dump t (oc:Lwtc.oc) = 
    File_system.stat t.fn >>= fun stat ->
    let length = Int64.of_int stat.st_size in
    Lwtc.log "dumping file of size %Li" length >>= fun () ->
    Llio.output_int64 oc length >>= fun () ->
    Lwt_io.with_file ~mode:Lwt_io.input t.fn
      (fun ic -> Llio.copy_stream ~length ~ic ~oc)
    >>= fun () ->
    Lwtc.log "raw_dump: done"

  let get_key_count t = Lwt.return 0 (* TODO: baardskeerder part *)
end: STORE)

