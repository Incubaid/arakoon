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

type tx_result = (v option, Arakoon_exc.rc * k) result
 
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
    Lwt_mutex.with_lock t.m (fun() -> BS.commit_last t.store)
    
  let close t = BS.close t.store
  
  let log t d u =
    let _exec tx :tx_result Lwt.t=
      begin 
        let rec _inner (tx: BS.tx) = function
          | Core.SET (k,v) ->
            Lwtc.log "set: %s" k >>= fun () -> 
            BS.set tx (pref_key k) v >>= fun () -> Lwt.return (OK None)
          | Core.DELETE k  -> 
            begin
              Lwtc.log "del: %s" k >>= fun () ->
              BS.delete tx (pref_key k) >>= function
                | NOK _ -> Lwt.return (NOK (Arakoon_exc.E_NOT_FOUND, k))
                | a -> Lwt.return (OK None)
            end
          | Core.DELETE_PREFIX k ->
            begin
              Lwtc.log "delete_prefix : %s" k >>= fun () ->
              BS.delete_prefix tx (pref_key k) >>= fun c ->
              let b = Buffer.create 8 in (* TODO: this means we're doing something wrong *)
              let () = Llio.int_to b c  in
              let v = Buffer.contents b in
              Lwt.return (OK (Some v))
            end
          | Core.ADMIN_SET (k, m_v) -> 
            begin
              let k' = pref_key ~_pf:__admin_prefix k in
              match m_v with
              | None -> BS.delete tx k' >>= fun _ -> Lwt.return (OK None)
              | Some v -> BS.set tx k' v >>= fun () -> Lwt.return (OK None)
            end
          | Core.ASSERT (k, m_v) -> 
            (*
              begin
              let pk = pref_key k in
              match m_v with
              | None -> 
                begin
                  BS.exists tx pk >>= fun b ->
                  let r = 
                    if b 
                    then TX_FAIL (Arakoon_exc.E_ASSERTION_FAILED,k)
                    else TX_SUCCESS None
                  in
                  Lwt.return r
                end
              | Some v -> 
                begin
                  BS.get tx pk >>= fun vo ->
                  let r = 
                    match vo with
                    | Some v' when v = v'-> TX_SUCCESS None
                    | _                  -> TX_FAIL (Arakoon_exc.E_ASSERTION_FAILED,k)
                  in
                  Lwt.return r
                end
            end
            *)
            begin
              BS.get tx (pref_key k) >>= fun r ->              
              let (r':tx_result) = match r with
                | OK v' -> 
                  if m_v <> (Some v') 
                  then NOK (Arakoon_exc.E_ASSERTION_FAILED, k)
                  else OK None
                | NOK k -> 
                  if m_v <> None 
                  then NOK (Arakoon_exc.E_ASSERTION_FAILED, k)
                  else OK None
              in
              Lwt.return r'
            end

          | Core.SEQUENCE s -> 
            Lwt_list.fold_left_s 
            (fun a u ->
              match a with
                | OK _ -> _inner tx u  (* throws away the intermediate result *)
                | a    -> Lwt.return a
            )
            (OK None)
              s 
          | Core.USER_FUNCTION (name, po) ->
            let f = Userdb.Registry.lookup name in
            f tx po >>= fun ro -> 
            Lwtc.log "Bstore.returning %s" (Log_extra.string_option_to_string ro) >>= fun () ->
            let a = match ro with 
              | None   -> OK None
              | Some v -> OK (Some v)
            in
            Lwt.return a

        in _inner tx u
      end
    in  
    Lwt_mutex.with_lock t.m (fun () -> BS.log_update t.store ~diff:d _exec)

  let is_read_only t = t.read_only
  
  let last_update t =
    BS.last_update t.store >>= fun m_last ->
    begin
      match m_last with
        | None -> Lwt.return None
        | Some (i_time, ups, committed) ->
          begin
            let tick_i = ITick.from_int64 i_time in
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

      
  let __prefix_keys t prefix max p =
    let prefix' = pref_key ~_pf:p prefix in
    Lwtc.log "prefix' = %S" prefix' >>= fun () ->
    BS.prefix_keys_latest t.store prefix' max >>= fun keys ->
    let u = List.map (unpref_key ~_pf:p) keys in
    
    Lwt.return u
  
  let prefix_keys t prefix max = 
    __prefix_keys t prefix max __prefix
    
  let admin_prefix_keys t prefix =
    __prefix_keys t prefix None __admin_prefix
  
  let last_entries t (t0:Core.ITick.t) (oc:Llio.lwtoc) = 
    let i0 = ITick.to_int64 t0 in
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
    Lwt_io.flush oc >>= fun () ->
    Lwtc.log "raw_dump: flushed & done"
      

  let get_key_count t = BS.key_count_latest t.store
end: STORE)

