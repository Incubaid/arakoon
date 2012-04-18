open Lwt

let iterate (sa:Unix.sockaddr) (i0:int64) (f: 'a -> int64 -> Baardskeerder.action list -> 'a Lwt.t) =
  let outgoing buf =
    Common.command_to buf Common.LAST_ENTRIES;
    Llio.int64_to buf i0
  in
  let incoming ic = 
    let rec loop last = 
      Lwt_io.printlf "loop" >>= fun () ->
      Sn.input_sn ic >>= fun i2 ->
      Lwt_io.printlf "%Li%!" i2 >>= fun () ->
      if i2 = (-1L) then
        begin
          Lwt.return ()
        end
      else
        begin
          Lwt.return ()
        end
    in
    loop None >>= fun () ->
    Lwt_io.printlf "done"
  in    
  Lwt_io.with_connection sa
    (fun (ic,oc) ->
      Common.prologue "ricky"  (ic,oc) >>= fun () ->
      Common.request oc outgoing >>= fun () ->
      Common.response ic incoming
    )


let main ip port i0 = 
  let sa = Network.make_address ip port in
  let i0 = 0L in
  let t () = iterate sa i0 
    (fun () i acs -> Lwt.return ())
  in
  Lwt_main.run (t ())


let _ = main "127.0.0.1" 4000 0L;;
    
