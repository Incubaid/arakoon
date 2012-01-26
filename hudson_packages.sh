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

export ARTEFACTS=artefacts
mkdir -p ${ARTEFACTS}
mv ../arakoon_*_amd64.deb ${ARTEFACTS}
mv ../libarakoon-ocaml-dev_1.0-dev_amd64.deb ${ARTEFACTS}

# now the python egg part
python setup.py bdist_egg

mv dist/arakoon-${BRANCH}-py2.?.egg ${ARTEFACTS}


