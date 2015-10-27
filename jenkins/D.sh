#!/bin/bash -xue

echo $PWD
eval `${opam_env}`
make
python test_it.py --with-xunit --xunit-file=${PWD}/testresults.xml ./server/shaky
