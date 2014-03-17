open OUnit
open Statistics

let (==:) x y = OUnit.cmp_float ~epsilon:0.0001 x y

let test_correctness0 () =
  let t0 = create_x_stats() in
  let () = update_x_stats t0 1.0 in
  let () = Printf.eprintf "t0=%s\n" (x_stats_to_string t0) in
  OUnit.assert_bool "min <= max" (t0.min <= t0.max);
  OUnit.assert_bool "avg" (t0.avg ==: 1.)


let test_correctness1 () =
  let t0 = create_x_stats() in
  let () = List.iter (update_x_stats t0) [1.0;0.9;1.1;1.0;0.8;1.2] in
  let () = Printf.eprintf "t0=%s\n" (x_stats_to_string t0) in
  OUnit.assert_bool "avg" (t0.avg ==:  1.);
  OUnit.assert_bool "min" (t0.min ==: 0.8)


let test_serialization () =
  let s =  Statistics.create () in
  let b = Buffer.create 80 in
  let () = Statistics.to_buffer b s in
  let bs = Buffer.contents b in
  let () = Printf.eprintf "bs=%S\n" bs in
  let s1 = Statistics.from_buffer (Llio.make_buffer bs 0) in
  OUnit.assert_equal ~printer:Statistics.string_of s s1

let suite = "statistics" >::: [
    "correctness0" >:: test_correctness0;
    "correctness1" >:: test_correctness1;
    "serialization" >:: test_serialization;
  ]
