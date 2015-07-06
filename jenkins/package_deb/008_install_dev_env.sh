#!/bin/bash -xue

which opam > /dev/null || { echo 'opam not found!'; exit 1; }
{ opam remote list | grep Incubaid/opam-repository-devel > /dev/null; } || { opam remote add incubaid-devel -k git git://github.com/Incubaid/opam-repository-devel.git; }

opam update -y
opam switch 4.02.2
eval `opam config env`

opam install -y \
    conf-libev.4-11 \
    camlbz2.0.6.0 \
    lwt.2.4.8 camltc.0.9.2 \
    ssl.0.5.0 \
    bisect.1.3 \
    quickcheck.1.0.2 \
    ounit.2.0.0 \
    nocrypto.0.5.0 \
    zarith.1.3 \
    cstruct.1.6.0 ctypes.0.4.1 ctypes-foreign.0.4.0 \
    snappy.0.1.0 \
    ocplib-endian.0.8 \
    uuidm.0.9.5
