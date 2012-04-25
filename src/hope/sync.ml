open Lwt
open Bstore 
open Baardskeerder

let _action2s = function
  | Set (k,v) -> Printf.sprintf "Set(%S,%S)" k v
  | Delete k  -> Printf.sprintf "Delete %S" k

let iterate f a0 ic = 
  let rec loop a last = 
    Sn.input_sn ic >>= fun i2 ->
    Lwtc.log "i2=%Li" i2 >>= fun () ->
    if i2 = (-1L) then
      begin
        Lwt.return ()
      end
    else
      begin
        Llio.input_list Core.input_action ic >>= fun actions ->          
        Lwtc.log "actions = [%s]" (String.concat ";" (List.map _action2s actions)) >>= fun () ->
        f a i2 actions >>= fun a' ->
        loop a' (Some i2)
      end
  in
  loop a0 None 
    
let remote_iterate (sa:Unix.sockaddr) cluster_id (i0:int64)
    (f: 'a -> int64 -> action list -> 'a Lwt.t)
    a0 
    =
  let outgoing buf =
    Common.command_to buf Common.LAST_ENTRIES;
    Llio.int64_to buf i0
  in
  let incoming ic = iterate f a0 ic 
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
        then Int64.succ ct
        else Int64.pred ct
    in
    remote_iterate sa cluster_id i0 
      (fun a i acs -> 
        let us = List.map action2update acs in
        let u = Core.SEQUENCE us in
        let d = true in
        BStore.log  log d u)
      Core.UNIT
    >>= fun () ->
    Lwt.return ()
  end
