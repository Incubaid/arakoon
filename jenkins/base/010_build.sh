#!/bin/bash -xue

eval `opam config env`

ocamlfind printconf
ocamlfind list | grep bz2
ocamlfind list | grep lwt
ocamlfind list | grep camltc

ocamlbuild -clean
ocamlbuild -use-ocamlfind arakoon.native arakoon.byte

#./arakoon.d.byte --run-all-tests-xml foobar.xml
./arakoon.native --run-all-tests-xml foobar.xml || true

#make coverage
#./arakoon.d.byte --run-all-tests
#./report.sh

# redo this for artifacts...
#ocamlbuild -use-ocamlfind arakoon.native arakoon.byte

echo "Fixup symlinks (absolute to relative)"
symlinks -c .

cp arakoon.native arakoon
