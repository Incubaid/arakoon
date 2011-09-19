open Client_cfg
open OUnit

let test_parsing () = 
  let lines = [
    "[global]";
   "cluster_id = sturdy";
   "cluster = sturdy_0,sturdy_1, sturdy_2  ";
   "\n";
   "[sturdy_0]";
   "client_port = 7080";
   "name = sturdy_0";
   "ip = 127.0.0.1";
   "\n";
   "[sturdy_1]";
   "client_port = 7081";
   "name = sturdy_1";
   "ip = 127.0.0.1";
   "\n";
   "[sturdy_2]";
   "ip = 127.0.0.1";
   "client_port = 7082";
   "name = sturdy_2"]
  in
  let contents = List.fold_left (fun acc v -> acc ^ "\n" ^ v) "" lines in
  let fn = "/tmp/client_cfg_test.ml" in
  let oc = open_out "/tmp/client_cfg_test.ml" in
  let () = output_string oc contents in
  let () = close_out oc in
  let cfg = ClientCfg.from_file "global" fn in
  let sa0 = ClientCfg.get cfg "sturdy_0" in
  OUnit.assert_equal sa0 ("127.0.0.1",7080)

let suite = "client_cfg" >:::[
  "parsing" >:: test_parsing;
]
