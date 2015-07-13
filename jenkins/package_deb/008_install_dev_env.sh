#!/bin/bash -xue

which opam > /dev/null || { echo 'opam not found!'; exit 1; }

opam switch 4.02.1
eval `opam config env`

# do this in 1 step, otherwise, you might not get what you want
opam install -y ssl.0.5.0 \
  conf-libev.4-11 \
  camlbz2.0.6.0 \
  snappy.0.1.0 \
  lwt.2.4.8 \
  camltc.0.9.2 \
  cstruct.1.7.0 \
  ctypes.0.4.1 \
  ctypes-foreign.0.4.0 \
  bisect.1.3 \
  ocplib-endian.0.8 \
  quickcheck.1.0.2 \
  nocrypto.0.4.0 \
  sexplib.112.24.01 \
  uuidm.0.9.5 \
  zarith.1.3
