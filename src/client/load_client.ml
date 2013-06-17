open Lwt

let load_scenario cfg_name x =
  let f cn = 
    Client_main.with_master_client 
      cfg_name 
      (fun client -> 
        let rec loop i = 
          if i = 1000 
          then Lwt.return ()
          else 
            begin
              Lwt_io.printlf "%04i: %i" cn i >>= fun () ->
              let key = Printf.sprintf "%i:%i_key" cn i in
              client # set key key >>= fun () -> 
              Lwt_unix.sleep 0.1   >>= fun () ->
              loop (i + 1)
            end
        in
        loop 0
      )
  in
  let cnis = 
    let rec loop  acc = function
      | 0 -> acc 
      | i -> loop (i :: acc) (i-1)
    in
    loop [] x
  in
  Lwt_list.iter_p f cnis

let main cfg_name n = Lwt_extra.run (load_scenario cfg_name n);0

  
