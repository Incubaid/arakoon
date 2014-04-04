#!/bin/bash -xue

sudo aptitude update || true

for PKG in help2man; do
    sudo aptitude install -yVDq $PKG
done
