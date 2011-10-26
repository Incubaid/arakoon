export BRANCH=$(hg branch)
export ARAKOON_BUILD_ENV="/opt/arakoon_build_end/${BRANCH}"
export PATH=${ARAKOON_BUILD_ENV}/OCAML/bin:${PATH}
export DEB_BUILD_OPTIONS=nocheck

fakeroot debian/rules clean
fakeroot debian/rules build
fakeroot debian/rules binary

export ARTIFACTS=artifacts
mkdir -p ${ARTIFACTS}
mv ../arakoon_1.0.0-1_amd64.deb ${ARTIFACTS}

# now the python egg part
python setup.py bdist_egg

mv dist/arakoon-${BRANCH}-py2.7.egg ${ARTIFACTS}

