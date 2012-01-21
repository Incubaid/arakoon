#!/bin/bash -xue

sudo aptitude update || true

for PKG in texlive texlive-latex-extra darcs git automake fakeroot python-epydoc python-setuptools debhelper dh-ocaml python-virtualenv graphviz; do
    sudo aptitude install -yVDq $PKG
done
