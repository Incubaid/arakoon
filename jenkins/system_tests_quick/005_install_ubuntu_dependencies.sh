#!/bin/bash -xue

sudo aptitude update || true

for PKG in libssl-dev; do
    sudo aptitude install -yVDq $PKG
done
