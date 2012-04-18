open Lwt
open Bstore

let iterate (sa:Unix.sockaddr) (i0:int64) (f: 'a -> int64 -> Baardskeerder.action list -> 'a Lwt.t) a0 =
  let outgoing buf =
    Common.command_to buf Common.LAST_ENTRIES;
    Llio.int64_to buf i0
  in
  let input_action (ic:Llio.lwtic) = 
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
      Common.prologue "ricky"  (ic,oc) >>= fun () ->
      Common.request oc outgoing >>= fun () ->
      Common.response ic incoming
    )

let sync sa log =
  begin
    let i0 = 0L in
    let do_actions (tx:BS.tx) acs = Lwt_list.iter_s 
      (function
        | Baardskeerder.Set (k,v) -> BS.set tx k v
        | Baardskeerder.Delete k  -> BS.delete tx k
      ) acs
    in 
    iterate sa i0 
      (fun a i acs -> 
        BS.log_update log ~diff:true 
          (fun tx -> do_actions tx acs)) 
      ()
    >>= fun () ->
    Lwt.return ()
  end

let main ip port i0 = 
  let sa = Network.make_address ip port in
  let action2s = function
    | Baardskeerder.Set (k,v) -> Printf.sprintf "Set(%S,%S)" k v
    | Baardskeerder.Delete k  -> Printf.sprintf "Delete %S" k
  in
  let alist2s acs = String.concat ";" (List.map action2s acs) in

  let t () = 
    let fn = "bla.bs" in
    BS.init fn >>= fun () ->
    BS.make fn >>= fun log ->
    sync sa log >>= fun () ->
    BS.close log
  in
  Lwt_main.run (t ())


let _ = main "127.0.0.1" 4000 0L;;
    
