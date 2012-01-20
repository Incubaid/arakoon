#!/bin/bash -xue

sudo aptitude update

for PKG in fakeroot python-epydoc python-setuptools debhelper dh-ocaml; do
    sudo aptitude install -yVDq $PKG
done
