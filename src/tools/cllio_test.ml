open OUnit

let test_compatibility_int64() = 
  let do_one v = 
    let s = String.create 8 in
    let () = Cllio.int64_to_buffer s 0 v in
    let v',_ = Llio.int64_from s 0 in
    Printf.printf "%Li:%S\n" v s;
    assert_equal ~msg: "phase1" v v';
    ()
      (*
	let b = Buffer.create 8 in
	let () = LLio.int64_to b v in
	let bs = Buffer.contents b in
	let v' = ...
      *)
  in
  let vs = [0L;1L;-1L;65535L;49152L] in
  List.iter do_one vs


let suite = 
  "Cllio" >:::[
    "compatibility_int64" >:: test_compatibility_int64;
  ]
