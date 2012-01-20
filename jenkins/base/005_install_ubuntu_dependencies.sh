#!/bin/bash -xue

sudo aptitude update || true

for PKG in python-epydoc; do
    sudo aptitude install -yVDq $PKG
done
