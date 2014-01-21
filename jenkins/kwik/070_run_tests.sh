#!/bin/bash -xue

echo $PWD
#source ./_virtualenv/bin/activate

python test_it.py --with-xunit --xunit-file=$PWD/kwik.xml \
  server.quick.TestConfig \
  server.quick.TestCmdTools \
  server.quick.system_tests_basic \
  server.quick.system_tests_ssl \
  server.quick.test_readonly 

