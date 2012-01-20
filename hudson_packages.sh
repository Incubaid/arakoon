#!/bin/bash
export BRANCH=$(hg branch)
export PREFIX="/var/hudson/workspace/ROOT"
export OCAML_HOME=${PREFIX}/OCAML
export LIBRARY_PATH=${OCAML_HOME}/lib:${LIBRARY_PATH}
export LD_LIBRARY_PATH=${OCAML_HOME}/lib:${LD_LIBRARY_PATH}
export PATH=${OCAML_HOME}/bin:${PATH}
export DEB_BUILD_OPTIONS=nocheck

set -e
which ocaml
fakeroot debian/rules clean
#fakeroot debian/rules build
fakeroot debian/rules binary

export ARTIFACTS=artifacts
mkdir -p ${ARTIFACTS}
mv ../arakoon_1.0.0-1_amd64.deb ${ARTIFACTS}

# now the python egg part
python setup.py bdist_egg

mv dist/arakoon-${BRANCH}-py2.?.egg ${ARTIFACTS}


