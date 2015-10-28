#!/bin/bash -xue

echo $PWD
eval `${opam_env}`
echo TEST_HOME=${TEST_HOME}
export PATH=${PATH}:${TEST_HOME}/apps/arakoon/bin

make clean
make
python test_it.py -v --with-xunit --xunit-file=${PWD}/testresults.xml ./server/shaky
