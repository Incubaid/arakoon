open Hope
open Lwt
open Core
module Hub = HUB(MemStore)

let run_lwt () = 
  let client hub id = 
    let rec loop i =
      if i = 0 
      then Lwt.return ()
      else
        begin
          Lwt_unix.sleep 0.7 >>= fun () ->
          let u = SET (Printf.sprintf "xxx%i" i , "yyy")  in
          Lwt_io.printlf "%s:updating %s%!" (X.id2s id) (X.update2s u) >>= fun () ->
          Hub.update hub id u >>= fun z ->
          Lwt_io.printlf "result = %s" (X.result2s z) >>= fun () ->
          loop (i-1)
        end
    in
    loop 10
  in
  let rec spawn hub i = 
    if i = 0 
    then Lwt.return ()
    else 
      let () = Lwt.ignore_result (client hub i) in
      spawn hub (i -1)
  in
    
  let main () = 
    let hub = Hub.create () in
    Hub.push_msg hub (X.M_TIMEOUT X.start);
    Lwt.join [ spawn hub 5;
               Hub.serve hub;
             ]
  in
  Lwt_main.run (main())


let () = run_lwt() 



