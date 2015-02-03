#!/bin/bash -xue

echo WORKSPACE=${WORKSPACE}
eval `opam config env`


ocamlfind printconf
ocamlfind list | grep bz2
ocamlfind list | grep lwt
ocamlfind list | grep camltc
ocamlfind list | grep snappy

make clean
make
