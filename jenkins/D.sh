#!/bin/bash -xue

./jenkins/common.sh

echo $PWD
eval `${opam_env}`
echo TEST_HOME=${TEST_HOME}
export PATH=${PATH}:${TEST_HOME}/apps/arakoon/bin

make clean
make
python test_it.py -v -s --with-xunit --xunit-file=${PWD}/testresults.xml ./server/shaky
