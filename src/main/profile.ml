let () = 
  Ocamlviz.init();
  Ocamlviz.wait_for_connected_clients 1;
  Test.test_correctness();;
