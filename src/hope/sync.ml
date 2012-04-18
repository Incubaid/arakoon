open Lwt
open Bstore 


let iterate (sa:Unix.sockaddr) cluster_id 
    (i0:int64) 
    (f: 'a -> int64 -> Baardskeerder.action list -> 'a Lwt.t) 
    a0 
    =
  let outgoing buf =
    Common.command_to buf Common.LAST_ENTRIES;
    Llio.int64_to buf i0
  in
  let input_action (ic:Lwtc.ic) = 
    Lwt_io.read_char ic >>= function
      | 's' -> 
        Llio.input_string ic >>= fun k ->
        Llio.input_string ic >>= fun v ->
        Lwt.return (Baardskeerder.Set(k,v))
      | 'd' ->
        Llio.input_string ic >>= fun k ->
        Lwt.return (Baardskeerder.Delete k)
      | c -> Llio.lwt_failfmt "input_action '%C' does not yield an action" c
  in
  let incoming ic = 
    let rec loop a last = 
      Sn.input_sn ic >>= fun i2 ->
      if i2 = (-1L) then
        begin
          Lwt.return ()
        end
      else
        begin
          Llio.input_list input_action ic >>= fun actions ->          
          f a i2 actions >>= fun a' ->
          loop a' (Some i2)
        end
    in
    loop a0 None 
  in    
  Lwt_io.with_connection sa
    (fun (ic,oc) ->
      Common.prologue cluster_id  (ic,oc) >>= fun () ->
      Common.request oc outgoing >>= fun () ->
      Common.response ic incoming
    )

let sync ip port cluster_id log =
  let sa = Network.make_address ip port in
  begin
    BS.last_update log >>= fun uo ->
    let i0 = match uo with 
      | None -> 0L 
      | Some (i,_,is_explicit) ->
        if is_explicit then i else (Int64.pred i) 
    in
    let do_actions (tx:BS.tx) acs = Lwt_list.iter_s 
      (function
        | Baardskeerder.Set (k,v) -> BS.set tx k v
        | Baardskeerder.Delete k  -> BS.delete tx k
      ) acs
    in 
    iterate sa cluster_id i0 
      (fun a i acs -> 
        BS.log_update log ~diff:true 
          (fun tx -> do_actions tx acs)) 
      ()
    >>= fun () ->
    Lwt.return ()
  end
