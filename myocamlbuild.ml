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
    let hg_version = input_line ch in
    let () = close_in ch in
    hg_version
  with | End_of_file -> "Not available"
    
let hg_revision = run_cmd "hg id -i"

let branch_version = run_cmd "hg branch"


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
    let template = "let hg_revision = %S\n" ^^
      "let compile_time = %S\n" ^^
      "let version = %S"
    in
    Printf.sprintf template hg_revision time branch_version
  in
  Cmd (S [A "echo"; Quote(Sh cmd); Sh ">"; P "version.ml"])

let before_space s =
  try
    String.before s (String.index s ' ')
  with Not_found -> s

(* this lists all supported packages *)
let find_packages () =
  List.map before_space (split_nl & run_and_read "ocamlfind list")

let path_to_bisect_instrument () =
  try
    let r = run_and_read "ocamlfind query bisect" in
    let r_stripped = List.hd(split_nl r ) in
    (* Printf.printf "\n\npath=%s\n\n\n" r_stripped ; *)
      r_stripped
  with _ -> "___could_not_find_bisect___"

let _ = dispatch & function
  | Before_options ->
    Options.ocamlc   := ocamlfind  & S[A"ocamlc";  A"-thread";];
    Options.ocamlopt := ocamlfind  & S[A"ocamlopt";A"-thread";];
    Options.ocamldoc := ocamlfind  & S[A"ocamldoc"];
    Options.ocamldep := ocamlfind  & A"ocamldep"
  | After_rules ->
    rule "version.ml" ~prod: "version.ml" make_version;
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
       "doc/states.eps";
      ];
    rule "Extract .tar.gz"
      ~dep:"3rd-party/%(f).tar.gz"
      ~stamp:"3rd-party/%(f).extracted"
      begin fun env _build ->
	let archive = env "3rd-party/%(f).tar.gz" in
	let () = Log.dprintf 0 "extracting %s" archive in
	let cmd =
	  Cmd (S[(*A"echo";*)
	    A"tar";
	    A"zxvf";
	    A archive;
	    A"--directory";A"3rd-party";
	  ]) in
	Seq[cmd;]
      end;
    rule "configure 3rd-party"
      ~dep:"3rd-party/%(dir).extracted"
      ~stamp:"3rd-party/%(dir).configured"
      begin fun env _build ->
	let dir =  env "%(dir)" in
	let () = Log.dprintf 0 "configuring %s" dir in
	let evil_cmd =
	  Printf.sprintf "cd 3rd-party/%s && ./configure " dir in
	let configure = Cmd (S[Sh evil_cmd;
			       A"--disable-bzip";
			       A"--disable-zlib";
			       A"--disable-shared";
			      ]) in
	Seq[configure;]
      end;
    rule "make 3rd-party"
      ~dep:"3rd-party/%(dir).configured"
      ~stamp:"3rd-party/%(dir).make"
      begin fun env _build ->
	let dir = env "%(dir)" in
	let () = Log.dprintf 0 "make 3rd-party %s" dir in
	let make =
	  Printf.sprintf "cd 3rd-party/%s && make" dir in
	let cmd = Cmd(Sh make) in
	Seq[cmd]
      end;

      (* how to compile C stuff that needs tc *)
    let tc_home ="3rd-party/tokyocabinet-1.4.45" in
    flag ["compile"; "c";]
      (S[
	A"-ccopt";A( "-I" ^ tc_home);
	A"-ccopt";A"-I../src/tools";
      ]);
    flag ["compile";"c";]
      (S[
	A"-ccopt";A"-msse4.2";
      ]);

    dep ["ocaml";"link"]["src/otc/libotc.a"];

    dep ["ocaml";"link"]["src/libcutil.a"];

    flag ["ocaml";"link"](
      S[A"-linkpkg";
	A"-ccopt"; A("-L" ^ tc_home);
	A"-ccopt"; A"-ltokyocabinet";
       ]);

    flag ["ocaml";"byte";"link"] (S[A"-custom";]);

    flag ["ocaml";"compile";"warn_error"]
       (S[A"-warn-error";A"A"]);

    (* dependency of wrapper on tc *)
    dep ["compile";"c";"file:src/otc/otc_wrapper.c"]
      ["3rd-party/tokyocabinet-1.4.45.make"];

    flag ["pp"; "camlp4of"] & S[A"-loc"; A"loc"] ;

    flag ["compile";"ocaml";"use_bisect"]
      (S[A"-I";A"+bisect"]);
    flag ["pp";"ocaml";"use_bisect"]
      (S[A"-no_quot";A(path_to_bisect_instrument()  ^ "/instrument.cmo")]);


    flag ["ocaml";"byte";"link";"use_bisect"]
      (S[ A"-I"; A"+bisect"; A"bisect.cma"; ]);

    flag ["ocaml";"native";"link";"program";"use_bisect"]
      (S[A"-I";A"+bisect";A"bisect.cmxa"; ]);

    flag ["link";"use_bisect"] (S[A"-ccopt";A"--coverage";]);

    flag ["pp";"file:src/tlog/tlogcommon.ml"]
      (S[A"camlp4o";A"pa_macro.cmo"]);
    flag ["pp";"small_tlogs";"file:src/tlog/tlogcommon.ml"] 
      (S[A"-DSMALLTLOG"]);

    (* For each ocamlfind package one inject the -package option when
    	* compiling, computing dependencies, generating documentation and
    	* linking. *)
    List.iter begin fun pkg ->
      flag ["ocaml"; "compile";  "pkg_"^pkg] & S[A"-package"; A pkg];
      flag ["ocaml"; "ocamldep"; "pkg_"^pkg] & S[A"-package"; A pkg];
      flag ["ocaml"; "doc";      "pkg_"^pkg] & S[A"-package"; A pkg];
      (* only use -package for linking programs, not for linking .cma or .cmxa libraries *)
      flag ["ocaml"; "link"; "program"; "pkg_"^pkg] & S[A"-package"; A pkg];
      flag ["ocaml"; "infer_interface"; "pkg_"^pkg] & S[A"-package"; A pkg];
    end (find_packages ());

  | _ -> ()
