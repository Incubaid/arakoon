#!/bin/bash -xue

which opam > /dev/null || { echo 'opam not found!'; exit 1; }
{ opam remote list | grep Incubaid/opam-repository-devel > /dev/null; } || { opam remote add incubaid-devel -k git git://github.com/Incubaid/opam-repository-devel.git; }

opam switch 4.00.1
eval `opam config env`

opam update -y
opam install conf-libev
opam install lwt camlbz2 camltc.0.5.1 bisect
