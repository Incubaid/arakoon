cd /opt/qbase3/var/tmp/arakoon-x/src/client/php/test
cat > /tmp/run_php_tests.sh << EOF
export PATH=/opt/qbase3/bin/:/opt/qbase3/apps/arakoon/bin:$PATH
export LD_LIBRARY_PATH=/opt/qbase3/lib
php all_nodes_down_test.php
php all_nodes_up_test.php
php invalid_client_configuration_test.php
php master_failover_test.php 
php one_node_down_test.php
echo ${WORKSPACE}/phpresults
mv *.xml ${WORKSPACE}/phpresults
EOF

sudo /bin/bash /tmp/run_php_tests.sh
