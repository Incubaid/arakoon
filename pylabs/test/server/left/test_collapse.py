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
from Compat import X
import arakoon
import time
import logging

from threading import Thread
from nose.tools import *

def _check_tlog_dirs(node, n):
    (home_dir, _, tlf_dir, head_dir) = Common.build_node_dir_names(node)

    tlogs = X.listFilesInDir( home_dir, filter="*.tlog" )
    tlxs  = X.listFilesInDir( tlf_dir,  filter="*.tlx" )
    logging.info("tlogs:%s", tlogs)
    logging.info("tlxs:%s", tlxs)
    print tlxs

    assert_equals(len(tlogs) + len(tlxs), n,
                  msg = "(%s + %s) should have %i file(s)" % (tlogs,tlxs,n))
    assert_true(X.fileExists(head_dir + "/head.db"))
    logging.info("tlog_dirs are as expected")

@Common.with_custom_setup(Common.setup_2_nodes_forced_master_mini, Common.basic_teardown)
def test_remote_collapse_witness_node():
    master = Common.node_names[0]
    witness = Common.node_names[1]
    n = 29876
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    Common.collapse(witness,1)
    logging.info("collapsing done")
    _check_tlog_dirs(witness,1)

    Common.stopOne(master)
    Common.wipe(master)
    Common.startOne(master)
    cli = Common.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")

@Common.with_custom_setup(Common.setup_2_nodes_mini, Common.basic_teardown)
def test_remote_collapse():
    zero = Common.node_names[0]
    one = Common.node_names[1]
    n = 29876
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    Common.collapse(zero,1)
    logging.info("collapsing done")
    _check_tlog_dirs(zero,1)

    Common.stopOne(one)
    Common.wipe(one)
    Common.startOne(one)
    cli = Common.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")

@Common.with_custom_setup(Common.setup_3_nodes, Common.basic_teardown)
def test_remote_collapse_under_load():
    n = 54321
    Common.iterate_n_times(n, Common.set_get_and_delete)
    logging.info("did %i sets, now do bench", n)
    cluster = Common._getCluster()
    path = cluster._getConfigFileName() + ".cfg"
    f = open("./outputFile", 'wb')
    bench = X.subprocess.Popen([Common.CONFIG.binary_full_path,
                              '-config', path ,'--benchmark',
                              '-scenario','master, set, set_tx, get',
                              '-max_n', '60000'],
                             stdout=f,
                             stderr=f)
    time.sleep(10.0)

    # bench is up to speed. collapse

    zero = Common.node_names[0]
    config = Common.getConfig(zero)
    cluster_id = Common.CONFIG.cluster_id
    ip = config['ip']
    port = config['client_port']
    f2 = open('collapse_output','wb')
    args = [Common.CONFIG.binary_full_path,
                                 '--collapse-remote', cluster_id,
                                 ip,port,str(5)]
    collapse = X.subprocess.Popen(args,
                                  stdout = f2,
                                  stderr = f2)

    time.sleep(5)
    #but stop while collapsing.
    Common.stopOne(zero)

    #cluster moves on
    Common.iterate_n_times(n, Common.set_get_and_delete)


    Common.startOne(zero)
    time.sleep(20)

    Common.assert_running_nodes(3)
    client = Common.get_client()
    m = client.whoMaster()
    logging.info("master is:%s", m)
    assert_true(m in Common.node_names)

    #
    bench.wait()
    collapse.wait()

@Common.with_custom_setup(Common.setup_3_nodes_mini, Common.basic_teardown)
def test_collapse_during_catchup():
    n = 54321
    t0 = time.time()
    Common.iterate_n_times(n, Common.set_get_and_delete)
    t1 = time.time()
    duration = t1 - t0
    zero = Common.node_names[0]
    Common.stopOne(zero)
    Common.iterate_n_times(n, Common.set_get_and_delete)
    Common.startOne(zero)
    logging.info("duration = %d", duration)

    time.sleep(duration / 3)

    Common.collapse(zero, n=5)
    cluster = Common._getCluster()


    client = cluster.getClient()

    def zero_state():
        state = client.getCurrentState(zero)
        return state

    state = None
    count = 0

    # collapsing and catchup will be racing.
    # the most annoying scenario is when catchup wins,
    # but whatever happens, the node needs to become a slave
    #

    try:
        while count < 100 and state != 'Slave_steady_state' :
            state = zero_state()
            logging.info("%i zero state:%s", count, state)
            count = count + 1
    except Exception,e :
        logging.info("Exception:", e)

    status = cluster.getStatusOne(zero)
    ok = status == X.AppStatusType.RUNNING
    if not ok:
        cfg = Common.getConfig(zero)
        log_dir = cfg['log_dir']
        log_file = '/'.join([log_dir, "%s.log" % (zero) ])
        tail = X.subprocess.check_output("tail -50 %s" % log_file, shell = True)
        for line in tail.split('\n'):
            logging.info("%s:%s" % (zero, line))


    assert_true(ok)

@Common.with_custom_setup(Common.setup_2_nodes_mini, Common.basic_teardown)
def test_remote_collapse_client():
    print
    cluster = Common._getCluster()
    cluster_id = cluster._getClusterId()
    config = cluster.getClientConfig()
    ccfg = X.arakoon_client.ArakoonClientConfig(cluster_id,
                                                config)
    client = X.arakoon_admin.ArakoonAdmin(ccfg)

    zero = Common.node_names[0]
    one = Common.node_names[1]
    n = 29876
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)

    client.collapse(zero,1)
    logging.info("collapsing done")
    _check_tlog_dirs(zero,1)


@Common.with_custom_setup(Common.setup_2_nodes_forced_master_mini, Common.basic_teardown)
def test_local_collapse_witness_node():
    master = Common.node_names[0]
    witness = Common.node_names[1]
    n = 29876
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)

    rc = Common.local_collapse(witness,1)
    assert_equal(rc,0)

    logging.info("collapsing done")


    Common.stopOne(master)
    Common.wipe(master)
    Common.startOne(master)
    cli = Common.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")

@Common.with_custom_setup(Common.setup_2_nodes_mini, Common.basic_teardown)
def test_local_collapse():
    logging.info("starting test_local_collapse")

    zero = Common.node_names[0]
    one = Common.node_names[1]
    n = 29876
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    rc= Common.local_collapse(zero,1)
    assert_equal(rc,0)
    head_name = '%s/%s/head/head.db' %(Common.data_base_dir, zero)
    logging.info(head_name)
    assert_true(X.fileExists(head_name))
    #
    logging.info("collapsing done")
    Common.stopOne(one)
    Common.wipe(one)
    Common.startOne(one)
    cli = Common.get_client()
    logging.info("cli class:%s", cli.__class__)
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")
