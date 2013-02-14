open Core
open Lwt
open Baardskeerder 
open Unix
open Interval
open Routing
    
let action2update = function
  | Set (k,v) -> Core.SET(unpref_key k,v)
  | Delete k -> Core.DELETE (unpref_key k)

let inflate_interval m_v = 
  let s = Pack.make_input m_v 4 in
  let iv = Interval.interval_from s in
  iv
    
let deflate_interval iv =
  let p = Pack.make_output 32 in
  let () = Interval.interval_to p iv in
  Pack.close_output p 

let _read_interval (store:BS.t) =
  BS.get_latest store (pref_key ~_pf:__admin_prefix Core.__interval_key) >>= function 
    | OK v  -> let iv = inflate_interval v in 
               Lwt.return (iv,v)
    | NOK k -> let iv = Interval.max  in 
               let v = deflate_interval iv in
               Lwt.return (iv,v )
     

module BStore = (struct
  type t = { m: Lwt_mutex.t; 
             store: BS.t; 
             mutable meta: string option; 
             read_only: bool;
             fn : string;
             mutable interval: (Interval.t * string);
           }

  type tx_result = (v option, Arakoon_exc.rc * k) result
 
  let init fn = 
    BS.init fn 
      
  let create fn read_only = 
    BS.make fn >>= fun store ->
    _read_interval store >>= fun interval ->
    let m = Lwt_mutex.create() in
    let r = {
      m; 
      store; 
      meta=None; 
      read_only; 
      fn;
      interval;
    } 
    in
    Lwt.return r
  
  let set_meta t s = Lwt.return (t.meta <- Some s)
  
  let get_meta t = Lwt.return t.meta
    
  let _outside k   = Lwt.return (NOK (Arakoon_exc.E_OUTSIDE_INTERVAL, k))
  let _not_found k = Lwt.return (NOK (Arakoon_exc.E_NOT_FOUND       , k))

  let _check_interval ?(nok=_outside)
      t k (ok: k -> 'a) = 
    let iv,_ = t.interval in
    let f = 
      if Interval.is_ok iv k
      then ok
      else nok
    in
    f k
    
  let commit t i = 
    Lwt_mutex.with_lock t.m (fun() -> BS.commit_last t.store)
    
  let close t = BS.close t.store
  
  let log t d u =
    let _exec tx :tx_result Lwt.t=
      begin 
        let rec _inner (tx: BS.tx) = function
          | Core.SET (k,v) ->
              begin
                Lwtc.log "set: %s" k >>= fun () -> 
                _check_interval t k 
                  (fun k -> BS.set tx (pref_key k) v >>= fun () -> Lwt.return (OK None))
              end
          | Core.DELETE k  -> 
            begin
              Lwtc.log "del: %s" k >>= fun () ->
              (*_check_interval t k (* TODO: re-enable this check, but do admin_sequence *)
                (fun k ->*)
                  BS.delete tx (pref_key k) >>= function
                    | NOK _ -> _not_found k
                    | a -> Lwt.return (OK None)
               (* ) *)
            end
          | Core.TEST_AND_SET (k,eo,wo) ->
              begin
                _check_interval t k 
                  (fun k ->
                    let pk = pref_key k in
                    let delete () = 
                      BS.delete tx pk >>= function
                        | OK _  -> Lwt.return (OK None)
                        | NOK _ -> Lwt.fail (Failure "can't happen")
                    and set w =
                      begin
                        BS.set tx pk w >>= fun () -> 
                        Lwt.return (OK eo)
                      end
                    in
                    match eo with 
                      | None ->
                          begin
                            BS.get tx pk >>= function
                              | OK v  -> Lwt.return (OK (Some v))
                              | NOK _ -> 
                                  begin
                                    match wo with
                                      | None   -> delete ()
                                      | Some w -> set w
                                  end
                          end
                      | Some e ->
                          begin
                            BS.get tx pk >>= function
                              | OK v  -> 
                                  if v = e 
                                  then 
                                    match wo with
                                      | None   -> delete() 
                                      | Some w -> set w
                                  else
                                    Lwt.return (OK (Some v))
                              | NOK _ -> 
                                  Lwt.return (OK None)                                  
                          end
                            
                  )
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
              Lwtc.log "admin_set: %s %s" k (Log_extra.string_option_to_string m_v) >>= fun () ->
              begin
                let () = 
                  if k = Core.__interval_key 
                  then 
                    match m_v with 
                      | None -> ()
                      | Some  v ->
                          let iv = inflate_interval v in
                          t.interval <- (iv,v)
                  else ()
                in
                let k' = pref_key ~_pf:__admin_prefix k in
                match m_v with
                  | None   -> BS.delete tx k'  >>= fun  _ -> Lwt.return (OK None)
                  | Some v -> BS.set tx k' v   >>= fun () -> 
                      begin
                        Lwt_log.debug_f "ADMIN_SET: %s %S" k v >>= fun () ->
                        Lwt.return (OK None)
                      end
              end
          | Core.SET_ROUTING_DELTA(left,sep,right) ->
              begin
                let k = pref_key ~_pf:__admin_prefix Core.__routing_key in
                BS.get tx k >>= fun rs ->
                let r' = match rs with
                  | NOK _ -> Routing.build ([(left,sep)],right)
                  | OK v  -> 
                      let input = Pack.make_input v 4 in
                      let r = Routing.routing_from input in
                      Routing.change r left sep right
                in
                let rc = Routing.compact r' in
                let out = Pack.make_output 80 in
                let () = Routing.routing_to out rc in
                let routing_s = Pack.close_output out in
                BS.set tx k routing_s >>= fun _ ->
                Lwt.return (OK None)

              end
          | Core.ASSERT (k, m_v) -> 
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
          | Core.ASSERT_EXISTS (k) ->
            begin
              BS.get tx (pref_key k) >>= fun r ->              
              let (r':tx_result) = match r with
                | OK v' -> OK None
                | NOK k -> NOK (Arakoon_exc.E_ASSERTION_FAILED, k)
              in
              Lwt.return r'
            end
          | Core.SEQUENCE s -> 
              begin
                Lwt_list.fold_left_s 
                  (fun a u ->
                    match a with
                      | OK _ -> _inner tx u  (* throws away the intermediate result *)
                      | a    -> Lwt.return a
                  )
                  (OK None)
                  s 
              end
          | Core.USER_FUNCTION (name, po) ->
              begin
                let f = Userdb.Registry.lookup name in
                f tx po >>= fun ro -> 
                Lwtc.log "Bstore.returning %s" (Log_extra.string_option_to_string ro) >>= fun () ->
                let a = match ro with 
                  | None   -> OK None
                  | Some v -> OK (Some v)
                in
                Lwt.return a
              end
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

  let get_interval t = t.interval
  
  let admin_get t k =
    if k = Core.__interval_key 
    then 
      let (_,v) = get_interval t in
      Lwt.return (Some v)
    else
      BS.get_latest t.store (pref_key ~_pf:__admin_prefix k) >>= function 
        | OK v -> begin Lwt_log.debug_f "admin_get %s --> %S" k v >>= fun () ->  Lwt.return (Some v) end
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
    Lwtc.log "range_rentries %s %b %s %b" 
      (Log_extra.string_option_to_string first) finc 
      (Log_extra.string_option_to_string last)  linc 
    >>= fun () ->
    _do_range_entries BS.range_entries_latest t first finc last linc max

  let rev_range_entries t first finc last linc max =
    Lwtc.log "rev_range_entries %s %b %s %b"
      (Log_extra.string_option_to_string first) finc
      (Log_extra.string_option_to_string last) linc
      >>= fun () ->
    _do_range_entries BS.rev_range_entries_latest t first finc last linc max


  let get_fringe t boundary direction =
    Lwtc.log "Bstore.get_fringe" >>= fun () ->
    let _inner () = 
      let size = Some 4 in
      match direction with
        | Routing.LOWER_BOUND -> 
            begin
              Lwtc.log "Bstore LOWER_BOUND fringe" >>= fun () ->
              let rec loop start acc = 
                rev_range_entries t start false boundary false size >>= fun kvs ->
                let acc' = kvs @ acc in
                Lwt.return  acc' 
              in
              loop (Some "z") []
          end
      | Routing.UPPER_BOUND -> 
          begin
            Lwtc.log "Bstore UPPER_BOUND fringe" >>= fun () ->
            let rec loop start acc = 
              range_entries t start false boundary false size >>= fun kvs ->
              let acc' = kvs @ acc in
              Lwt.return acc' 
            in
            loop None []
          end
    in
    Lwt_mutex.with_lock t.m _inner >>= fun kvs ->
    Lwt_log.debug_f "get_fringe yields %i kvs" (List.length kvs) >>= fun () ->
    Lwt_list.iter_s (fun (k,v) -> Lwt_log.debug_f "k:%s" k) kvs >>= fun () ->
    Lwt.return kvs
    
      
                  

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

  let dump t = (* TODO: need a better name *)
    let iv,_ = get_interval t in
    Lwt_io.printlf "interval:\t%s" (Interval.to_string iv) >>= fun ()->
    Lwt_io.printf  "last    :\t" >>= fun () ->
    last_update t >>= function
      | None       -> Lwt_io.printl  "None"
      | Some (i,_) -> Lwt_io.printlf "Some (%s, _ )" (Core.ITickUtils.tick2s i)
    (* ... *)

end: STORE)

