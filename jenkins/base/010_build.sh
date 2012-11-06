echo WORKSPACE=${WORKSPACE}
BUILD_ENV=${WORKSPACE%${JOB_NAME}}
echo BUILD_ENV=${BUILD_ENV}
eval `${BUILD_ENV}/ROOT/OPAM/bin/opam --root ${BUILD_ENV}/ROOT/OPAM_ROOT config -env`
PATH=$PATH:${BUILD_ENV}/ROOT/OCAML/bin

ocamlfind printconf
ocamlfind list | grep bz2
ocamlfind list | grep lwt
ocamlfind list | grep camltc
ocamlfind list | grep baardskeerder


make clean
make

python test_it.py -s --with-xunit --xunit-file=foobar.xml test.server.quick

