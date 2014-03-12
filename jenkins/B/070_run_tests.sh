#!/bin/bash -xue

echo $PWD

python test_it.py --with-xunit --xunit-file=${PWD}/B.xml \
  server.left.system_tests_long_left \
  server.left.test_collapse \
  server.left.test_concurrent_collapse_fails \
  server.left.test_copy_db_to_head
