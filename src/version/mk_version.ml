let run_cmd cmd () =
  try
    let ch = Unix.open_process_in cmd in
    let line = input_line ch in
    let () = close_in ch in
    line
  with | End_of_file -> "Not available"

let run_cmd_multiline cmd () =
  let rec input_lines acc ch =
    match input_line ch with
    | line -> input_lines (line :: acc) ch
    | exception End_of_file -> List.rev acc
  in

  let ch = Unix.open_process_in cmd in
  let lines = input_lines [] ch in
  let () = close_in ch in
  lines


let list_dependencies () =
  let versions =
    run_cmd_multiline {| dune installed-libraries | sed 's/[ ]*[(]version: \(.*\)[)]/ \1/' |} ()
    |> List.fold_left (fun acc line ->
        let fields = String.split_on_char ' ' line in
        let pkg = List.hd fields in
        let version = String.concat " " (List.tl fields) in
        Hashtbl.add acc pkg version;
        acc
      ) (Hashtbl.create 0)
  in

  run_cmd_multiline "cd ../../../..; dune external-lib-deps @install" ()
  |> List.tl
  |> List.map (fun line ->
      let pkg = List.tl (String.split_on_char ' ' line) |> List.hd in
      let version = match Hashtbl.find_opt versions pkg with
        | Some v -> v
        | None -> "<unknown>"
      in
      Printf.sprintf "%-20s\t%16s" pkg version
    )
  |> String.concat "\n"

let time () =
  let tm = Unix.gmtime (Unix.time()) in
  Printf.sprintf "%02d/%02d/%04d %02d:%02d:%02d UTC"
    (tm.tm_mday) (tm.tm_mon + 1) (tm.tm_year + 1900)
    tm.tm_hour tm.tm_min tm.tm_sec

let major_minor_patch_modifier () =
  let tag_version = run_cmd "git describe --tags --dirty" in
  let tv =
    try tag_version ()
    with _ -> ""
  in
  let (major, minor, patch, modif) =
    try
      Scanf.sscanf tv "%i.%i.%i-%s" (fun ma mi p dev -> (ma,mi,p, Some dev))
    with _ ->
      try
        Scanf.sscanf tv "%i.%i.%i" (fun ma mi p -> (ma,mi,p,None))
      with _ ->
        (-1,-1,-1,None)
  in
  let modif_s =
    match modif with
    | None -> "None"
    | Some s -> Printf.sprintf "Some %S" s
  in
  Printf.sprintf "(%d, %d, %d, %s)" major minor patch modif_s

let stringify v = Printf.sprintf "%S" v
let id = fun x -> x

let fields = [ "git_revision", run_cmd "git describe --all --long --always --dirty", stringify
             ; "compile_time", time, stringify
             ; "machine", run_cmd "uname -mnrpio", stringify
             ; "compiler_version", run_cmd "ocamlopt -version", stringify
             ; "(major, minor, patch, modifier)", major_minor_patch_modifier, id
             ; "dependencies", list_dependencies, stringify
             ]

let vals = List.map (fun (n, f, r) -> (n, f (), r)) fields
let lines = List.map (fun (n, v, r) -> Printf.sprintf "let %s = %s" n (r v)) vals


let () = Printf.printf "%s\n" (String.concat "\n" lines)
