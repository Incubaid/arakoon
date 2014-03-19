#!/bin/bash -xue

echo $PWD

python test_it.py --with-xunit --xunit-file=${PWD}/C.xml \
  server.right.system_tests_preferred_masters \
  server.right.system_tests_anomaly  \
  server.right.test_catchup_exercises \
  server.right.system_tests_learner \
  server.right.test_extension \
  server.right.system_tests_long_right
