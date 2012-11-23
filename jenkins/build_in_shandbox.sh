echo WORKSPACE=${WORKSPACE}
BUILD_ENV=${WORKSPACE%${JOB_NAME}}
echo BUILD_ENV=${BUILD_ENV}
PATH=${BUILD_ENV}/ROOT/OCAML/bin:$PATH
eval `${BUILD_ENV}/ROOT/OPAM/bin/opam --root ${BUILD_ENV}/ROOT/OPAM_ROOT config -env`


ocamlfind printconf
ocamlfind list | grep bz2
ocamlfind list | grep lwt
ocamlfind list | grep camltc

ocamlbuild -clean
ocamlbuild -use-ocamlfind arakoon.native

./buildInSandbox.sh

