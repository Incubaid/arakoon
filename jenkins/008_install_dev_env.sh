#!/bin/bash -xue

which opam > /dev/null || { echo 'opam not found!'; exit 1; }
{ opam remote list | grep Incubaid/opam-repository-devel > /dev/null; } || { opam remote add incubaid-devel -k git git://github.com/Incubaid/opam-repository-devel.git; }

opam update -y
opam switch 4.02.1
eval `opam config env`

opam install -y "ssl.0.4.7"
opam install -y conf-libev
opam install -y camlbz2
opam install -y snappy
opam install -y "lwt.2.4.7"
opam remove -y camltc
opam install -y "camltc.999"
opam install -y bisect
opam install -y quickcheck
