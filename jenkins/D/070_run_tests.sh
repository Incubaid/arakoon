#!/bin/bash -xue

echo $PWD

python test_it.py --with-xunit --xunit-file=${PWD}/testresults.xml ./server/shaky
