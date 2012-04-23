open Lwt
open Bstore 
open Baardskeerder

let iterate (sa:Unix.sockaddr) cluster_id 
    (i0:int64) 
    (f: 'a -> int64 -> action list -> 'a Lwt.t) 
    a0 
    =
  let outgoing buf =
    Common.command_to buf Common.LAST_ENTRIES;
    Llio.int64_to buf i0
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
          Llio.input_list Core.input_action ic >>= fun actions ->          
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

let sync ip port cluster_id (log : BStore.t) =
  let sa = Network.make_address ip port in
  begin
    BStore.last_update log >>= fun luo ->
    let i0 =  
      match luo with
      | None -> 0L
      | Some (Core.TICK ct,cuo) -> 
        if cuo = None 
        then Int64.pred ct 
        else ct
    in
    let rec action2update = function
      | Set(k,v) -> Core.SET(k,v)
      | Delete k -> Core.DELETE k
    in

    iterate sa cluster_id i0 
      (fun a i acs -> 
        let us = List.map action2update acs in
        let u = Core.SEQUENCE us in
        let d = true in
        BStore.log  log d u)
      Core.UNIT
    >>= fun () ->
    Lwt.return ()
  end
