#!/bin/bash -xue

source travis/env.sh

sudo apt-get update -qq
sudo apt-get install -qq build-essential m4 ocaml-nox libbz2-dev libev-dev camlp4-extra

mkdir ${OPAM_DIR}

pushd ${OPAM_DIR}
git clone git://github.com/OCamlPro/opam.git src
pushd src
./configure --prefix=${OPAM_DIR}
make
make install

popd
popd

which opam
opam init

opam remote add incubaid-devel --kind git git://github.com/Incubaid/opam-repository-devel.git

eval `opam config -env`

opam install conf-libev
echo 'y' | opam install lwt
opam install camlbz2
opam install ounit
opam install camltc
