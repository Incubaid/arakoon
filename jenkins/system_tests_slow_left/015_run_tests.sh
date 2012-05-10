BUILD_ENV=/var/hudson/workspace/ROOT/OCAML
echo BUILD_ENV=${BUILD_ENV}

export PATH=$BUILD_ENV/bin:$PATH
export LD_LIBRARY_PATH=$BUILD_ENV/lib
export LIBRARY_PATH=$BUILD_ENV/lib

python test_it.py -s --with-xunit --xunit-file=foobar.xml test.server.left
