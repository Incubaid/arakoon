(* of course, this will only work on the right OS ;) *)

module Node = struct
  let exec = "./arakoon.native" 
  let names = ["arakoon_0";"arakoon_1";"arakoon_2"] 
  let cfg_name = (Unix.getcwd()) ^ "/cfg/arakoon.ini" 

  let root = "/tmp/arakoon"
  let _node_cmd fmt name = 
    let cmd = Printf.sprintf fmt exec name cfg_name in
    print_endline cmd; 
    Sys.command cmd
  
  let start name = _node_cmd "%s --node %s -config %s -daemonize" name
    
  let stop name = _node_cmd "pkill -f '%s --node %s -config %s'" name

  let start_all () =
   List.iter (fun n -> ignore (start n)) names 

  let stop_all () =
    List.iter (fun n -> ignore (stop n)) names

  let monitor_all () =
    let tab x = Printf.sprintf 
      "--tab-with-profile=Default -e 'tail -f %s/%s/%s.log' -t '%s' " 
      root x x x 
    in
    let tabs = List.map tab names in
    let cmd = List.fold_left (^) "gnome-terminal " tabs in
      print_endline cmd;
      Sys.command cmd

  let mkdirs () =
    let mkdir x = 
      let dir_name = Printf.sprintf "%s/%s" root x in
  Unix.mkdir dir_name 0o755
    in List.map mkdir names

  let clean_dirs () = 
    let dir_name name = Printf.sprintf "%s/%s" root name in
    List.iter 
      (fun name -> ignore (Sys.command ("rm -rf " ^ (dir_name name)))) 
      names
 
end
