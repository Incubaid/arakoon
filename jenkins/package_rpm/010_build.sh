#!/bin/bash -xue

eval `opam config env`

ln -s '../../' ./redhat/SOURCES/arakoon | true
ARAKOON_HOME=`pwd`
cd ~/
rm rpmbuild | true
ln -s $ARAKOON_HOME/redhat rpmbuild
cd rpmbuild
rpmbuild -ba SPECS/arakoon.spec

