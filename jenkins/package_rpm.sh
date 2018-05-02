#!/bin/bash -xue

export START=${PWD}
echo START=${START}
rm -rf ${START}/rpmbuild/
make clean
mkdir -p ${START}/rpmbuild/SOURCES
cd ${START}/rpmbuild/SOURCES/
ln -f -s ${START} ./arakoon
cd ${START}
ls -lR .
#chown root:root ./redhat/SPECS/arakoon.spec
#make
rpmbuild --define "_topdir ${START}/rpmbuild" -bb ./redhat/SPECS/arakoon.spec
