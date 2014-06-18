#!/bin/bash -xue

sudo aptitude update || true

for PKG in libssl-dev \
 texlive texlive-latex-extra \
 git help2man python-epydoc graphviz \
 libsnappy-dev libleveldb-dev; do
    sudo aptitude install -yVDq $PKG
done
