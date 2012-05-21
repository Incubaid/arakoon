open OUnit
open Core

let update_serialization () = 
  let test update = 
    let buf0 = Buffer.create 32 in
    let () = update_to buf0 update in
    let bufs = Buffer.contents buf0 in
    let u2,_ = update_from bufs 0 in
    OUnit.assert_equal ~printer:update2s update u2
  in
  List.iter test [
    SET("key_set", "value_set");
    DELETE "key_delete";
    ASSERT ("key_assert", None);
    ADMIN_SET ("key_admin", Some "admin_value");
    SEQUENCE [SET ("k0","v0)");
              SET ("k1","v1");
              SET ("k2","v2");
              SET ("k3","v3");
              SET ("k4","v4");
              SET ("k5","v5");
             ]
  ]


  
let suite = 
  "Core" >:::[
    "update_serialization" >:: update_serialization;
  ]
