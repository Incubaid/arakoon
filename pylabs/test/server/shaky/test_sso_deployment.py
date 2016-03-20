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
from arakoon.ArakoonExceptions import *
import arakoon
import logging
import time
import subprocess
from nose.tools import *
import os
import random
from threading import Thread, Condition
from Compat import X

@Common.with_custom_setup( Common.setup_1_node_forced_master, Common.basic_teardown )
def test_sso_deployment():
    pass
    """ the scaling scenario from 1 node to 3 nodes the way A-server does it (eta : 2200 s) """
    global test_failed
    test_failed = False

    def write_loop ():
        Common.iterate_n_times( 10000,
                                Common.retrying_set_get_and_delete )

    def large_write_loop ():
        Common.iterate_n_times( 280000,
                                Common.retrying_set_get_and_delete,
                                startSuffix = 1000000 )

    write_thr1 = Common.create_and_start_thread ( write_loop )
    def non_retrying_write_loop ():
        Common.iterate_n_times( 10000,
                                Common.set_get_and_delete,
                                startSuffix = 2000000  )

    Common.add_node( 1 )
    cl = Common._getCluster()
    cl.setLogLevel("debug")

    Common.regenerateClientConfig(Common.cluster_id)

    Common.restart_nodes_wf_sim( 1 )
    n1 = Common.node_names[1]
    logging.info("going to start %s", n1)
    Common.startOne(n1 )

    Common.create_and_wait_for_thread_list ( [ large_write_loop ] )

    Common.add_node( 2 )
    cl = Common._getCluster()
    cl.setLogLevel("debug")
    cl.forceMaster(None )
    logging.info("2 node config without forced master")

    Common.regenerateClientConfig(Common.cluster_id)

    Common.restart_nodes_wf_sim( 2 )
    Common.startOne( Common.node_names[2] )
    time.sleep( 0.3 )
    Common.assert_running_nodes ( 3 )

    write_thr3 = Common.create_and_start_thread ( non_retrying_write_loop )

    write_thr1.join()
    write_thr3.join()

    assert_false ( test_failed )

    Common.assert_running_nodes( 3 )

@Common.with_custom_setup( Common.default_setup, Common.basic_teardown )
def test_expand_shrink_cluster():
    # test to see the effect of a stranger talking to a cluster

    # add a fourth node
    Common.add_node(3)

    # regenerate configs & restart all four
    Common.regenerateClientConfig(Common.cluster_id)
    Common.restart_nodes_wf_sim(4)

    # remove the fourth node, restart the rest
    cl = Common._getCluster()
    cl.removeNode(Common.node_names[3])
    Common.regenerateClientConfig(Common.cluster_id)
    Common.restart_nodes_wf_sim(3)

    # now there should a cluster of 3 nodes that know each other
    # and 1 stranger which will want to talk with this cluster
    time.sleep(20)
    Common.assert_running_nodes(4)
    client = Common.get_client()
    master = client.whoMaster()
    assert_true ( master in Common.node_names[0:3] )
    client.nop()
