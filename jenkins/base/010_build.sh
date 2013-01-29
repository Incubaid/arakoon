echo WORKSPACE=${WORKSPACE}
BUILD_ENV=${WORKSPACE%${JOB_NAME}}
echo BUILD_ENV=${BUILD_ENV}
PATH=${BUILD_ENV}/ROOT/OCAML/bin:$PATH
eval `${BUILD_ENV}/ROOT/OPAM/bin/opam config env -r ${BUILD_ENV}/ROOT/OPAM_ROOT`


ocamlfind printconf
ocamlfind list | grep bz2
ocamlfind list | grep lwt
ocamlfind list | grep baardskeerder

make clean
make

python test_it.py -s --with-xunit --xunit-file=foobar.xml test.server.quick

