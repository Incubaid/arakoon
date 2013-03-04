#!/bin/bash -xue

eval `opam config env`

ocamlfind printconf
ocamlfind list | grep bz2
ocamlfind list | grep lwt
ocamlfind list | grep camltc

ocamlbuild -clean
ocamlbuild -use-ocamlfind arakoon.native arakoon.byte

echo "Fixup symlinks (absolute to relative)"
symlinks -c .

#make coverage 
#./arakoon.d.byte --run-all-tests-xml foobar.xml
./arakoon.native --run-all-tests-xml foobar.xml
./report.sh
# redo this for artifacts...
ocamlbuild -use-ocamlfind arakoon.native arakoon.byte
mkdir -p doc/python/client
epydoc --html --output ./doc/python/client --name Arakoon --url http://www.arakoon.org --inheritance listed --graph all src/client/python
mkdir -p doc/python/client/extension
epydoc --html --output ./doc/python/client/extension --name "Arakoon PyLabs client extension" --url http://www.arakoon.org --inheritance listed --graph all extension/client/*.py
mkdir -p doc/python/server/extension
epydoc --html --output ./doc/python/server/extension --name "Arakoon PyLabs server extension" --url http://www.arakoon.org --inheritance listed --graph all extension/server/*.py
cp arakoon.native arakoon
