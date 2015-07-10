#!/bin/bash -xue

which opam > /dev/null || { echo 'opam not found!'; exit 1; }
{ opam remote list | grep Incubaid/opam-repository-devel > /dev/null; } || { opam remote add incubaid-devel -k git git://github.com/Incubaid/opam-repository-devel.git; }

opam update -y
opam switch 4.02.1
eval `opam config env`

# do this in 1 step, otherwise, you might not get what you want
opam install -y "ssl.0.4.7" \
  conf-libev \
  camlbz2 \
  snappy \
  "lwt.2.4.8" \
  "camltc.999" \
  bisect \
  quickcheck
