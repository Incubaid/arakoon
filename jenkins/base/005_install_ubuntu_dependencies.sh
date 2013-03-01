#!/bin/bash -xue

sudo aptitude update || true

for PKG in texlive texlive-latex-extra git python-epydoc graphviz; do
    sudo aptitude install -yVDq $PKG
done
