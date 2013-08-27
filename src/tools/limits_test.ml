open OUnit
open Limits

let test_rusage () =
  let rec loop i =
    if i = 0
    then ()
    else
      let r = Limits.get_rusage () in
      let () = Printf.printf "maxrss=%Li; ixrss=%Li; idrss=%Li; isrss=%Li\n"
        r.maxrss r.ixrss r.idrss r.isrss
      in
      loop (i-1)
  in
  loop 1000

let suite = "Limits">:::["test_rusage" >:: test_rusage]
