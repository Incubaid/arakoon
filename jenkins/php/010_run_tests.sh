#!/bin/bash -xue

USER=`whoami`

cd src/client/php/test

export PATH=/opt/qbase3/bin/:/opt/qbase3/apps/arakoon/bin:$PATH
export LD_LIBRARY_PATH=/lib/x86_64-linux-gnu:/opt/qbase3/lib64

echo $LD_LIBRARY_PATH && php all_nodes_down_test.php
#sudo php all_nodes_up_test.php
#sudo php invalid_client_configuration_test.php
#sudo php master_failover_test.php
#sudo php one_node_down_test.php
#sudo php stats_test.php

sudo chown $USER *.xml

mkdir -p ${WORKSPACE}/phpresults
mv *.xml ${WORKSPACE}/phpresults
