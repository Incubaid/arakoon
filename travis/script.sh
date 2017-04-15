#!/bin/bash -xue

source travis/env.sh

make
./arakoon.native --run-all-tests > test_output 2>&1

# This seems not to work...
# RES=$?

tail -n 500 test_output

if ! (tail -n1 test_output | grep ^OK);
then
        echo "Detected failure"
        exit 1
else
        echo "Detected success"
        exit 0
fi
