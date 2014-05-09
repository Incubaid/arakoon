open Ocamlbuild_pack
open Ocamlbuild_plugin
open Unix

let pdflatex = A"pdflatex"
let run_and_read = Ocamlbuild_pack.My_unix.run_and_read

(* ocamlfind command *)
let ocamlfind x = S[A"ocamlfind"; x]

let run_cmd cmd =
  try
    let ch = Unix.open_process_in cmd in
    let line = input_line ch in
    let () = close_in ch in
    line
  with | End_of_file -> "Not available"


let output_cmd cmd =
  let acc = ref [] in
  let ch = Unix.open_process_in cmd in
  try
    let rec loop () =
      let line = input_line ch in
      let () = acc := line :: !acc in
      loop ()
    in
    loop ()
  with | End_of_file ->
    let () = close_in ch in
    List.rev (!acc)

let git_revision = run_cmd "git describe --all --long --always --dirty"
let tag_version = run_cmd "git describe --tags --exact-match --dirty"
let branch_version = run_cmd "git describe --all"

let machine = run_cmd "uname -mnrpio"

let compiler_version = run_cmd "ocamlopt -version"

let dependencies = output_cmd "opam list -i | grep 'lwt\\|ounit\\|camltc\\|snappy\\|ssl\\|camlbz2'"

let split s ch =
  let x = ref [] in
  let rec go s =
    let pos = String.index s ch in
    x := (String.before s pos)::!x;
    go (String.after s (pos + 1))
  in
  try
    go s
  with Not_found -> !x

let split_nl s = split s '\n'

let time =
  let tm = Unix.gmtime (Unix.time()) in
  Printf.sprintf "%02d/%02d/%04d %02d:%02d:%02d UTC"
    (tm.tm_mday) (tm.tm_mon + 1) (tm.tm_year + 1900)
    tm.tm_hour tm.tm_min tm.tm_sec

let make_version _ _ =
  let cmd =
    let template = "let git_revision = %S\n" ^^
                     "let compile_time = %S\n" ^^
                     "let machine = %S\n" ^^
                     "let compiler_version = %S\n" ^^
                     "let major = %i\n" ^^
                     "let minor = %i\n" ^^
                     "let patch = %i\n" ^^
                     "let dependencies = %S\n"
    in
    let major,minor,patch =
      try
        Scanf.sscanf tag_version "%i.%i.%i" (fun ma mi p -> (ma,mi,p))
      with _ ->
        try Scanf.sscanf branch_version "heads/%i.%i" (fun ma mi -> (ma,mi,-1))
        with _ ->
          (* This one matches what's on Jenkins slaves *)
          try Scanf.sscanf branch_version "remotes/origin/%i.%i" (fun ma mi -> (ma, mi, -1))
          with _ -> (-1,-1,-1)
    in
    Printf.sprintf template git_revision time machine compiler_version major minor patch
      (String.concat "\\n" dependencies)
  in
  Cmd (S [A "echo"; Quote(Sh cmd); Sh ">"; P "arakoon_version.ml"])

let before_space s =
  try
    String.before s (String.index s ' ')
  with Not_found -> s

let path_to_bisect_instrument () =
  try
    let r = run_and_read "ocamlfind query bisect" in
    let r_stripped = List.hd(split_nl r ) in
    (* Printf.printf "\n\npath=%s\n\n\n" r_stripped ; *)
    r_stripped
  with _ -> "___could_not_find_bisect___"

let _ = dispatch & function
    | After_rules ->
      rule "arakoon_version.ml" ~prod: "arakoon_version.ml" make_version;
      rule "LaTeX to PDF"
        ~prod:"%.pdf"
        ~dep:"%.tex"
        begin fun env _build ->
          let tex = env "%.tex" in
          (* let pdf = env "%.pdf" in *)
          let tags = tags_of_pathname tex ++ "compile" ++ "LaTeX" ++ "pdf" in
          let cmd = Cmd(S[pdflatex;A"-shell-escape";T tags;P tex;A"-halt-on-error"]) in
          Seq[cmd;]
        end;
      dep ["compile";"LaTeX";"pdf";]
        ["doc/introduction.tex";
         "doc/client.tex";
         "doc/consistency.tex";
         "doc/protocol.tex";
         "doc/restarting.tex";
         "doc/part3.tex";
         "doc/state_machine.tex";
         "doc/user_functions.tex";
         "doc/states.eps";
         "doc/nursery.tex";
        ];

      (* how to compile C stuff that needs tc *)
      flag ["compile"; "c";]
        (S[
            A"-ccopt";A"-I../src/tools";
          ]);
      flag ["compile";"c";]
        (S[
            A"-ccopt";A"-msse4.2";
          ]);

      dep ["ocaml";"link";"is_main"]["src/libcutil.a"];

      flag ["ocaml";"link";"is_main"](
        S[A"-thread";
          A"-linkpkg";
          A"src/libcutil.a";
         ]);
      flag ["ocaml";"compile";] (S[A"-thread"]);

      flag ["ocaml";"byte";"link"] (S[A"-custom";]);

      flag ["ocaml";"compile";"warn_error"]
        (S[A"-w"; A"+A-4-27@33";]);

      flag ["pp"; "camlp4of"] & S[A"-loc"; A"loc"] ;

      flag ["compile";"ocaml";"use_bisect"]
        (S[A"-I";A"+bisect"]);


      flag ["ocaml";"byte";"link";"use_bisect"]
        (S[ A"-I"; A"+bisect"; A"bisect.cma"; ]);

      flag ["ocaml";"native";"link";"program";"use_bisect"]
        (S[A"-I";A"+bisect";A"bisect.cmxa"; ]);

      flag ["link";"use_bisect"] (S[A"-ccopt";A"--coverage";]);

      flag ["pp";"ocaml";"use_log_macro"] (A"logger_macro.cmo");
      dep ["ocaml"; "ocamldep"; "use_log_macro"] ["logger_macro.cmo"];

      flag ["pp";"ocaml";"use_bisect"]
        (S[A"-no_quot";A(path_to_bisect_instrument()  ^ "/instrument.cmo")]);

      flag ["pp";"use_macro";"small_tlogs";
            "file:src/tlog/tlogcommon.ml"] (S[A"-DSMALLTLOG"]);
      flag ["library";"use_thread"](S[A"-thread"]);
    | _ -> ()
