open OUnit
let suite = 
  "correctness" >::: 
    [ Bstore_test.suite;
      Core_test.suite;
    ]
