open Lwt
open Node_cfg

let mr_proper () =
  Node.stop_all();
  Node.clean_dirs();
  Node.mkdirs()

let test_large_tlog_collection_restart () = 
  let fill client = 
    let rec loop i = 
      if i = 100000 
      then Lwt.return () 
      else
	begin
	  Lwt_io.printlf "%i" i >>= fun () ->
	  let key = Printf.sprintf "key%i" i in
	  let value = Printf.sprintf "value%i" i in
	  client # set key value >>= fun () ->
          loop (i+1)
	end
    in
    loop 0
  in
  let _ = mr_proper () in
  Node.start_all();
  Unix.sleep 1;
  let _ = Client_main.with_master_client "cfg/arakoon.ini" fill in
  Node.stop_all();
  Node.start_all();
  Unix.sleep 10;
  (* iterate_n_times( 100, set_get_and_delete ) *)
  Client_main.with_master_client "cfg/arakoon.ini" 
    (fun c -> c # set "XXX" "XXX");;

      
let test_lost_update () =
  let _ = mr_proper () in
  Node.start_all();
  Unix.sleep 1;
  let _ = Client_main.with_master_client "cfg/arakoon.ini" 
    (fun c -> c # set "xxx" "xxx")
  in 
  let cfgs,_,_,_,_ = Node_cfg.read_config "cfg/arakoon.ini" in
  let master = Lwt_main.run (Client_main.find_master cfgs) in
  let others = List.filter (fun n -> n <> master) Node.names in
  Node.stop_all();
  List.iter (fun n -> ignore (Node.start n)) others;
  Unix.sleep 1;
  Client_main.with_master_client "cfg/arakoon.ini"
    (fun c -> c # get "xxx" >>= fun v -> Lwt_io.printlf "value=%s" v);
  Node.stop_all();;

  
