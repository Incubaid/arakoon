

BUILD_ENV=../ROOT/OCAML
echo BUILD_ENV=${BUILD_ENV}

export PATH=$BUILD_ENV/bin:$PATH
export LD_LIBRARY_PATH=$BUILD_ENV/lib
export LIBRARY_PATH=$BUILD_ENV/lib

make clean
make

python test_it.py -s --with-xunit --xunit-file=foobar.xml test.server.quick

# redo this for artifacts...
#ocamlbuild -use-ocamlfind barakoon.native barakoon.byte
#mkdir -p doc/python/client
#epydoc --html --output ./doc/python/client --name Arakoon --url http://www.arakoon.org --inheritance listed --graph all src/client/python
#mkdir -p doc/python/client/extension
#epydoc --html --output ./doc/python/client/extension --name "Arakoon PyLabs client extension" --url http://www.arakoon.org --inheritance listed --graph all extension/client/*.py
#mkdir -p doc/python/server/extension
#epydoc --html --output ./doc/python/server/extension --name "Arakoon PyLabs server extension" --url http://www.arakoon.org --inheritance listed --graph all extension/server/*.py
#cp arakoon.native arakoon
