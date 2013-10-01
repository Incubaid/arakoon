#!/bin/bash -xue

which opam > /dev/null || { echo 'opam not found!'; exit 1; }
{ opam remote list | grep Incubaid/opam-repository-devel > /dev/null; } || { opam remote add incubaid-devel -k git git://github.com/Incubaid/opam-repository-devel.git; }

opam switch 4.00.1
eval `opam config env`

opam update -y
opam pin camltc none || true
opam remove -y ounit
opam install conf-libev 
opam install -y camlbz2 
opam install -y "lwt.2.4.3"
opam install -y "camltc.0.7.0"
opam install -y bisect
