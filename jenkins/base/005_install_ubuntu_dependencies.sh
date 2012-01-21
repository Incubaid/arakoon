#!/bin/bash -xue

sudo aptitude update || true

for PKG in texlive darcs automake fakeroot python-epydoc python-setuptools debhelper dh-ocaml python-virtualenv; do
    sudo aptitude install -yVDq $PKG
done
