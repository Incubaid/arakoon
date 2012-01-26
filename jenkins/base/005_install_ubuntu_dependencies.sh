#!/bin/bash -xue

sudo aptitude update || true

for PKG in texlive texlive-latex-extra darcs git automake fakeroot python-epydoc python-setuptools debhelper dh-ocaml python-virtualenv graphviz ruby1.9.1 rubygems1.9.1 ruby1.9.1-dev libxml2-dev libxslt-dev; do
    sudo aptitude install -yVDq $PKG
done

gem1.9.1 install --user-install github-upload
