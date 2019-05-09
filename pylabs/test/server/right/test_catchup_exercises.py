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
from ..quick import system_tests_basic
from arakoon.ArakoonExceptions import *
import arakoon
import logging
import time
import subprocess
from threading import Thread
from nose.tools import *
from Compat import X
from arakoon_ext.client import ArakoonClient

def _scenario():
    def do_one(n, max_wait):
        logging.info("do_one(%i,%f)", n, max_wait)
        Common.iterate_n_times(n, Common.simple_set)
        logging.info("sets done, stopping")
        Common.stop_all()
        logging.info("stopped all nodes")
        nn = Common.node_names[1]
        Common.wipe(nn)

        Common.start_all()
        logging.info("started all")

        counter = 0
        up2date = False
        cli = Common.get_client ()
        while not up2date and counter < max_wait :
            time.sleep( 1.0 )
            counter += 1
            up2date = cli.expectProgressPossible()
            logging.info("up2date=%s", up2date)

        cli.dropConnections()
        if counter >= max_wait :
            raise Exception ("Node did not catchup in a timely fashion")

    n = 2000
    w = 8 # 250/s should be more than enough even for dss driven vmachines
    for i in range(5):
        do_one(n,w)
        n = n * 2
        w = w * 2




@Common.with_custom_setup(Common.setup_2_nodes_forced_master_mini, Common.basic_teardown)
def test_catchup_exercises():
    _scenario()


def test_catchup_mixed_config():
    pass
    """
    catchup where 2 nodes have different rollover points.
    (It's a configuration error that's bound to happen)
    """
    Common.data_base_dir = '/'.join([X.tmpDir,'arakoon_system_tests' ,
                                         "test_catchup_mixed_config"])

    def setup(c_id,tlog_max_entries):
            node_0 = "%s_0" % c_id
            c_nodes = [node_0]
            c_home = "/".join([Common.data_base_dir, c_id])
            node_dir = "/".join([Common.data_base_dir, node_0])
            X.removeDirTree(node_dir)
            c = Common.setup_n_nodes_base(c_id, c_nodes,False, c_home,
                                          Common.node_msg_base_port,
                                          Common.node_client_base_port,
                                          extra = {'tlog_max_entries':str(tlog_max_entries)}
            )
            return c

    def wait(c0):
        system_tests_basic._wait_for_master(c0)

    def _inner(c0):
        Common.regenerateClientConfig(id0)
        c0.start()
        wait(c0)

        c0_client = c0.getClient()

        for x in range(100):
            c0_client.set("key_%i" % x, "value_%i" % x)

        bad = 'nineteen'
        c0.addNode(bad,
                   clientPort = Common.node_client_base_port + 1,
                   messagingPort = Common.node_msg_base_port + 1,
                   home = '/'.join([Common.data_base_dir, bad]),
                   logLevel = "debug", isLocal = True
        )

        bad
        cfg = c0._getConfigFile()
        cfg.set('global','tlog_max_entries', str(13))
        c0._saveConfig(cfg)
        c0.createDirs(bad)
        c0.catchupOnly(bad)

        c0.stop()
        c0.start()

        wait(c0)
        Common.regenerateClientConfig(id0)
        logging.info("status:%s", c0.getStatus())

        c0_client = c0.getClient()
        logging.info('who_master:%s', c0_client.whoMaster())
        stats = c0_client.statistics()
        node_is = stats['node_is']
        logging.info("node_is:%s", node_is)
        lo = min(node_is.values())
        hi = max(node_is.values())
        assert_true(hi-lo <= 1)

        for x in range(100):
            c0_client.set("x","x")

    id0 = 'thirteen'
    c0 = setup(id0, 13)
    try:
        _inner(c0)
    finally:
        pass
        c0.stop()
        c0.tearDown()
        c0.remove()

@Common.with_custom_setup(Common.setup_3_nodes_forced_master_mini_rollover_on_size,
                          Common.basic_teardown)
def test_catchup_rollover_on_size():
    _scenario()
    lagger = Common.node_names[2]
    print ("now with collapsing as well")
    Common.stopOne(lagger)
    Common.iterate_n_times(1234, Common.simple_set)
    Common.collapse(Common.node_names[0], 20)
    Common.stopOne(Common.node_names[1])
    Common.startOne(lagger)
    time.sleep(20)
    cli = Common.get_client ()
    ok = cli.expectProgressPossible()
    assert_true(ok)
    Common.stop_all()
    head_dir = Common.build_node_dir_names(lagger)[3]
    head_file = head_dir + "/head.db"
    print head_file
    exists = X.fileExists(head_file)
    assert_false(exists)
    Common.assert_last_i_in_sync(Common.node_names[0], lagger)

@Common.with_custom_setup(Common.setup_3_nodes_forced_master_mini_rollover_on_size,
                          Common.basic_teardown)
def test_issue125():
    n = 31234
    Common.iterate_n_times(n, Common.simple_set)
    slave = Common.CONFIG.node_names[1]

    Common.collapse(slave,10)
    Common.stopOne(slave)

    Common.iterate_n_times(n, Common.simple_set) # others move forward

    dir_names = Common.build_node_dir_names(slave)
    db_dir = dir_names[0]
    log_dir = dir_names[1]
    tlx_dir =dir_names[2]


    X.removeFile('%s/035.tlog' % db_dir)
    X.removeFile('%s/034.tlx' % tlx_dir)
    X.removeFile('%s/%s.db' % (db_dir, slave))
    # we have a cluster with head.db, no db and no tlog file.

    # can we launch ?

    Common.startOne(slave, ['-autofix'])
    time.sleep(20)
    Common.assert_running_nodes(3)
