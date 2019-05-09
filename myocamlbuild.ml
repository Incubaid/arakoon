(* Findlib dependencies *)
let dependencies = [ "lwt";
                     "oUnit";
                     "camltc";
                     "snappy";
                     "ssl";
                     "bz2";
                     "ocplib-endian";
                     "redis";
                     "uri";
                     "core";
                   ]

(* Enabled compiler warnings, argument for '-w', see `man ocamlc` *)
let warnings = "+A"
(* Enabled compiler errors, argument for '-warn-error' *)
let errors = "+A"
(* List of files for which warning 4,
 *
 *     Fragile pattern matching: matching that will remain com‐
 *         plete even if additional constructors are added to one of the
 *         variant types matched.
 *
 * is disabled.
 *)
let allow_fragile_pattern_matches = [
  "logger_macro.ml";
  "src/tools/file_system.ml";
  "src/node/node_cfg.ml";
  "src/tools/server.ml";
  "src/node/catchup.ml";
  "src/tlog/tlc2.ml";
  "src/msg/tcp_messaging.ml";
  "src/client/client_protocol.ml";
  "src/client/client_main.ml";
  "src/nursery/nursery_main.ml";
  "src/system/startup.ml";
]

open Ocamlbuild_pack
open Ocamlbuild_plugin
open Unix

let run_cmd cmd () =
  try
    let ch = Unix.open_process_in cmd in
    let line = input_line ch in
    let () = close_in ch in
    line
  with | End_of_file -> "Not available"

let list_dependencies () =
  let query pkg =
    let package = Findlib.query pkg in
    (pkg, package.Findlib.version, package.Findlib.description)
  in
  let pkgs = List.map query dependencies in
  let lines = List.map (fun (pkg, version, descr) ->
    let tabs = if String.length pkg < 8 then "\t\t" else "\t" in
    Printf.sprintf "%s%s%12s\t%s" pkg tabs version descr)
    pkgs
  in
  String.concat "\n" lines

let time () =
  let tm = Unix.gmtime (Unix.time()) in
  Printf.sprintf "%02d/%02d/%04d %02d:%02d:%02d UTC"
    (tm.tm_mday) (tm.tm_mon + 1) (tm.tm_year + 1900)
    tm.tm_hour tm.tm_min tm.tm_sec

let major_minor_patch () =
  let tag_version = run_cmd "git describe --tags --exact-match --dirty 2>/dev/null"
  and branch_version = run_cmd "git describe --all" in

  let (major, minor, patch) =
    try
      Scanf.sscanf (tag_version ()) "%i.%i.%i" (fun ma mi p -> (ma,mi,p))
    with _ ->
      let bv = branch_version () in
      try
        Scanf.sscanf bv "heads/%i.%i" (fun ma mi -> (ma,mi,-1))
      with _ ->
        (* This one matches what's on Jenkins slaves *)
        try
          Scanf.sscanf bv "remotes/origin/%i.%i" (fun ma mi -> (ma, mi, -1))
        with _ -> (-1,-1,-1)
  in
  Printf.sprintf "(%d, %d, %d)" major minor patch


let make_version _ _ =
  let stringify v = Printf.sprintf "%S" v
  and id = fun x -> x in

  let fields = [ "git_revision", run_cmd "git describe --all --long --always --dirty", stringify
               ; "compile_time", time, stringify
               ; "machine", run_cmd "uname -mnrpio", stringify
               ; "compiler_version", run_cmd "ocamlopt -version", stringify
               ; "(major, minor, patch)", major_minor_patch, id
               ; "dependencies", list_dependencies, stringify
               ]
  in
  let vals = List.map (fun (n, f, r) -> (n, f (), r)) fields in
  let lines = List.map (fun (n, v, r) -> Printf.sprintf "let %s = %s\n" n (r v)) vals in
  Echo (lines, "arakoon_version.ml")


let path_to_bisect () =
  try
    let bisect_pkg = Findlib.query "bisect" in
    bisect_pkg.Findlib.location
  with Findlib.Findlib_error _ -> "__could_not_find_bisect__"


let _ = dispatch & function
     | Before_options ->
        Options.use_ocamlfind := true
    | After_rules ->
      rule "arakoon_version.ml" ~prod:"arakoon_version.ml" make_version;

      (* how to compile C stuff *)
      flag ["compile";"c";]
        (S[
            A"-ccopt"; A"-I../src/tools";
            A"-ccopt"; A"-msse4.2";
            A"-ccopt"; A"-Wall";
            A"-ccopt"; A"-Wextra";
            A"-ccopt"; A"-Werror";
            A"-ccopt"; A"-ggdb3";
            A"-ccopt"; A"-Wcast-align";
            A"-ccopt"; A"-O2";
            (* Optionally add something like -march=core2 -mtune=corei7-avx for
             * a minor extra performance gain in CRC32 calculations *)
          ]);

      dep ["ocaml";"link";"is_main"]["src/libcutil.a"];

      flag ["ocaml";"link";"is_main"](
        S[A"-linkpkg"; A"src/libcutil.a";
         ]);

      flag ["ocaml";"byte";"link"] (S[A"-custom";]);

      flag ["ocaml";"compile";"warn_error"]
        (S[A"-w"; A warnings; A"-warn-error"; A errors]);

      flag ["pp";"ocaml";"use_log_macro"] (A"logger_macro.cmo");
      dep ["ocaml"; "ocamldep"; "use_log_macro"] ["logger_macro.cmo"];

      flag ["ocaml"; "compile"; "use_bisect"]
        (S[A"-package"; A"bisect"]);
      flag ["ocaml"; "link"; "use_bisect"]
        (S[A"-package"; A"bisect"]);
      flag ["pp"; "ocaml"; "use_bisect"; "maybe_use_bisect"]
        (S[A"str.cma"; A(path_to_bisect () ^ "/bisect_pp.cmo")]);

      flag ["pp";"use_macro";"small_tlogs";
            "file:src/tlog/tlogcommon.ml"] (S[A"-DSMALLTLOG"]);

      flag ["ocaml"; "compile"; "native"] (S[A"-inline"; A"999"]);

      List.iter (fun f ->
        let f' = Printf.sprintf "file:%s" f in
        flag ["ocaml"; "compile"; f'] (S[A"-w"; A"-4"; A"-warn-error"; A"-4"]))
        allow_fragile_pattern_matches

    | _ -> ()
