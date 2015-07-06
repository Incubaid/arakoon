#!/bin/bash -xue

which opam > /dev/null || { echo 'opam not found!'; exit 1; }
{ opam remote list | grep Incubaid/opam-repository-devel > /dev/null; } || { opam remote add incubaid-devel -k git git://github.com/Incubaid/opam-repository-devel.git; }

opam update -y
opam switch 4.02.2
eval `opam config env`

opam install -y ssl conf-libev camlbz2 snappy lwt camltc bisect quickcheck uuidm
