#!/bin/bash -xue

./jenkins/common.sh

python test_it.py -v -s --with-xunit --xunit-file=${PWD}/testresults.xml ./server/shaky
