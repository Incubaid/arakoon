open OUnit
let suite = 
  "correctness" >::: 
    [ Bstore_test.suite;
      Core_test.suite;
      Arakoon_remote_client_test.suite;
    ]
