open OUnit
open Node_cfg
open Node_cfg
let test_correctness () =

  let cfg = Node_cfg.read_config !config_file in
  OUnit.assert_equal cfg.cluster_id "ricky"


let suite = "node_cfg" >::: ["correctness" >:: test_correctness]
