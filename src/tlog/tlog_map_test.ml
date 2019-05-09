open Tlog_map.TlogMap

let make_item (i,n,is_archive) = {i;n;is_archive}

let test_tlogs_to_collapse () =
  let tlog_map = {
      i_to_tlog_number = List.map
                           make_item [
                             (12000L,12,false);
                             (11000L,11,false);
                             (10000L,10,false);
                             ( 9000L, 9,false);
                             ( 8000L, 8,false);
                             ( 7000L, 7,false);
                             ( 6000L, 6,false);
                             ( 5000L, 5,false);
                             ( 4000L, 4,false);
                             ( 3000L, 3,false);
                             ( 2000L, 2,false);
                             ( 1000L, 1,false);
                             (    0L, 0,false);
                           ];
      tlog_dir = "tlog_dir";
      tlx_dir = "tlx_dir";
      tlog_max_entries = 1000;
      tlog_max_size = 5_000_000;
      node_id = "node_id";
      tlog_size = 400;
      tlog_entries = 393;
      tlog_number = 12;
      should_roll = false;
      lock = Lwt_mutex.create ();
    }
  in
  let head_i = 0L in
  let last_i = 12393L  in
  let tlogs_to_keep = 1 in
  let r = tlogs_to_collapse tlog_map head_i last_i tlogs_to_keep in
  let open To_string in
  let printer = To_string.option (pair string_of_int Sn.string_of) in
  let expected = Some (12,12_002L) in
  OUnit.assert_equal expected r ~printer

let test_find_start () =
  let i_to_tlog_number =
    List.map
      make_item [
        (12000L,12,false);
        (11000L,11,false);
        (10000L,10,false);
        ( 9000L, 9,false);
        ( 8000L, 8,false);
        ( 7000L, 7,false);
        ( 6000L, 6,false);
        ( 5000L, 5,false);
        ( 4000L, 4,false);
        ( 3000L, 3,false);
        ( 2000L, 2,false);
        ( 1000L, 1,false);
        (    0L, 0,false);
      ]
  in
  let printer items =
    let itemss = List.map (fun item -> Printf.sprintf "{i:%Li;...}" item.i) items
    in Printf.sprintf "[%s]" (String.concat "; " itemss)
  in
  let to_remove, _to_keep =
    _find_start 12000L i_to_tlog_number
  in
  let expected = List.tl i_to_tlog_number |> List.rev in
  OUnit.assert_equal expected to_remove ~printer ~msg:"test1";
  let to_remove2, _to_keep2 =
    _find_start 11002L i_to_tlog_number
  in
  let expected2 = i_to_tlog_number |> List.tl |> List.tl |> List.rev in
  OUnit.assert_equal expected2 to_remove2 ~printer ~msg:"test2";

  let to_remove3, _to_keep3 =
    _find_start 12002L i_to_tlog_number
  in
  OUnit.assert_equal expected to_remove3 ~printer ~msg:"test3";
  ()

open OUnit

let suite =
  "tlog_map">:::[
      "tlogs_to_collapse">:: test_tlogs_to_collapse;
      "find_start" >:: test_find_start;
    ]
