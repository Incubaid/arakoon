#!/bin/bash -xue

env | sort

git submodule init
git submodule update

echo $PWD
echo TEST_HOME=${TEST_HOME}
export PATH=${PATH}:${TEST_HOME}/apps/arakoon/bin

make clean build
