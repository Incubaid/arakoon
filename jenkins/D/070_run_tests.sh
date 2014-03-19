#!/bin/bash -xue

echo $PWD

python test_it.py --with-xunit --xunit-file=${PWD}/D.xml  \
  server.shaky.test_inject_as_head \
  server.shaky.test_243 \
  server.shaky.test_restart_master_long \
  server.shaky.test_272 \
  server.shaky.test_restart_single_slave_long \
  server.shaky.test_db_defrag \
  server.shaky.test_sso_deployment

