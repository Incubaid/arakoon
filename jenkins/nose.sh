#!/bin/bash -xue

./jenkins/common.sh
echo "nose.sh:PARAMETERS=$@"
python test_it.py -v --with-xunit --xunit-file=${PWD}/testresults.xml $@
