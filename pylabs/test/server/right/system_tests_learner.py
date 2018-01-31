"""
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""



from .. import system_tests_common as Common
from nose.tools import assert_true
import time
import logging
from Compat import X

@Common.with_custom_setup(Common.setup_2_nodes_mini, Common.basic_teardown)
def test_learner():
    op_count = 5432
    Common.iterate_n_times(op_count, Common.simple_set)
    cluster = Common._getCluster(Common.cluster_id)
    logging.info("adding learner")
    name = Common.node_names[2]
    (db_dir, log_dir, tlf_dir, head_dir) = Common.build_node_dir_names(name)
    cluster.addNode(name,
                    Common.node_ips[2],
                    clientPort = Common.node_client_base_port + 2,
                    messagingPort = Common.node_msg_base_port + 2,
                    logDir = log_dir,
                    tlfDir = tlf_dir,
                    headDir = head_dir,
                    logLevel = 'debug',
                    home = db_dir,
                    isLearner = True)
    cfg = cluster._getConfigFile()
    logging.info("cfg=%s", X.cfg2str(cfg))
    cluster.disableFsync([name])
    cluster.addLocalNode(name)
    cluster.createDirs(name)
    cluster.startOne(name)

    time.sleep(1.0)

    Common.assert_running_nodes(3)
    time.sleep(op_count / 1000 + 1 ) # 1000/s in catchup should be no problem
    #use a client ??"
    Common.stop_all()
    i1 = int(Common.get_last_i_tlog(name))
    assert_true(i1 >= op_count - 1)

    Common.start_all()
    time.sleep(1.0)
    Common.iterate_n_times(1000, Common.simple_set)
    Common.stop_all()

    i2 = int(Common.get_last_i_tlog(name))
    assert_true(i2 >= i1 + 900)
    i_normal_node = int(Common.get_last_i_tlog(Common.node_names[0]))
    assert_true (i2 >= i_normal_node - 100) # it should approximately follow the normal node
