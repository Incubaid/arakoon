open OUnit
open Routing

let test_serialization () = 
  let repr =[("left"),"k";],"right" in
  let routing = Routing.build repr in
  Printf.printf "original=%s\n" (Routing.to_s routing);
  let b = Buffer.create 127 in
  Routing.routing_to b routing ;
  let flat = Buffer.contents b in
  let inflated,_ = Routing.routing_from flat 0 in
  Printf.printf "reinflated=%s\n" (Routing.to_s inflated);
  OUnit.assert_equal routing inflated ~printer:Routing.to_s;
  ()
  

let suite = "routing" >:::["serialization" >:: test_serialization]
