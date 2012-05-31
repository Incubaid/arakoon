open Ocamlbuild_plugin

let _ = dispatch & function
 |After_rules ->
     flag ["ocaml";"compile";"use_thread"](S[A"-thread"]);
     flag ["ocaml";"link";"use_thread"](S[A"-thread"]);
 | _ -> ()

