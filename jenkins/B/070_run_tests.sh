#!/bin/bash -xue

echo $PWD

python test_it.py --with-xunit --xunit-file=${PWD}/B.xml ./server/left
