"""
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
nFITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
"""

from .. import system_tests_common as Common
from arakoon.ArakoonExceptions import *
import arakoon
import logging
import time
import subprocess
from threading import Thread
from nose.tools import *


@Common.with_custom_setup( Common.default_setup, Common.basic_teardown )
def test_restart_master_long ():
    restart_iter_cnt = 10
    def write_loop ():
        Common.iterate_n_times( 100000,
                                Common.retrying_set_get_and_delete,
                                failure_max=2*restart_iter_cnt,
                                valid_exceptions=[ArakoonSockNotReadable,
                                                  ArakoonNotFound,
                                                  ArakoonGoingDown
                                                  ] )

    def restart_loop ():
        Common.delayed_master_restart_loop( restart_iter_cnt ,
                                            1.5 * Common.lease_duration )
    global test_failed
    test_failed = False
    Common.create_and_wait_for_thread_list( [restart_loop, write_loop] )

    cli = Common.get_client()
    time.sleep(2.0)
    key = "key"
    value = "value"
    cli.set(key, value)
    set_value = cli.get(key)
    assert_equals(  value, set_value ,
        "Key '%s' does not have expected value ('%s' iso '%s')" % (key, set_value, value) )

    Common.stop_all()
    Common.start_all()
    Common.stop_all()
    node_names = Common.node_names

    Common.assert_last_i_in_sync( node_names[0], node_names[1] )
    Common.assert_last_i_in_sync( node_names[2], node_names[1] )
    Common.compare_stores( node_names[0], node_names[1] )
    Common.compare_stores( node_names[2], node_names[1] )
    cli.dropConnections()
    logging.info("end of `test_restart_master_long`")

@Common.with_custom_setup(Common.setup_3_nodes_mini, Common.basic_teardown)
def test_243():
    node_names = Common.node_names
    zero = node_names[0]
    one = node_names[1]
    two = node_names[2]
    npt = 1000
    n = 5 * npt + 200

    logging.info("%i entries per tlog", npt)
    logging.info("doing %i sets", n)
    Common.iterate_n_times(n, Common.simple_set)


    logging.info("did %i sets, now collapse all ", n)
    Common.collapse(zero,1)
    Common.collapse(one,1)
    Common.collapse(two,1)

    logging.info("set %i more", n)
    Common.iterate_n_times(n, Common.simple_set)

    logging.info("stopping %s", zero)
    Common.stopOne(zero)
    logging.info("... and set %s more ",n)
    Common.iterate_n_times(n, Common.simple_set)

    client = Common.get_client()
    stats = client.statistics()
    node_is = stats['node_is']
    logging.info("node_is=%s",node_is)
    logging.info("collapse 2 live nodes")
    Common.collapse(one,1)
    Common.collapse(two,1)
    Common.startOne(zero)

    stats = client.statistics ()
    node_is = stats['node_is']
    mark = max(node_is.values())
    catchup = True
    t0 = time.time()
    timeout = False
    logging.info("wait until catchup is done ....")
    count = 0

    while catchup:
        time.sleep(10)
        stats = client.statistics()
        node_is = stats['node_is']
        logging.info("node_is=%s", node_is)
        lowest = min(node_is.values())
        if lowest >= mark or count == 10:
            catchup = False
        count = count + 1

    stats = client.statistics()
    node_is = stats['node_is']
    logging.info("done waiting")
    logging.info("OK,node_is=%s", node_is)
    logging.info("now collapse %s", zero)
    rc = Common.collapse(zero,1)
    # if it does not throw, we should be ok.
    if rc :
        msg = "rc = %s; should be 0" % rc
        raise Exception(msg)
    logging.info("done")


@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_272():
    """
    test_272 : arakoon can go down during log rotation, but you need to have load to reproduce it (eta: 1110s)
    """
    node = Common.node_names[0]
    cluster = Common._getCluster()
    path = '%s.cfg' % cluster._getConfigFilePath()
    logging.info('path=%s', path)
    bench = subprocess.Popen([Common.binary_full_path, '-config', path ,'--benchmark'])
    time.sleep(10.0) # give it time to get up to speed
    rc = bench.returncode
    if rc <> None:
        raise Exception ("benchmark should not have finished yet.")

    for i in range(100):
        Common.rotate_log(node, 1, False)
        time.sleep(0.2)
        # Note:
        # At this point, we expect one node to be running, as well as the
        # benchmark process. The `assert_running_nodes` procedure uses
        # 'pgrep -c' which counts the number of 'arakoon' processes running, so
        # we should pass *2* here, despite only a single node process should be
        # running.
        Common.assert_running_nodes(2)
        rc = bench.returncode
        if rc <> None:
            raise Exception ("benchmark should not have stopped")

    Common.assert_running_nodes(2)
    logging.info("now wait for benchmark to finish")
    rc = bench.wait()
    if rc <> 0:
        raise Exception("benchmark exited with rc = %s" % rc)


@Common.with_custom_setup(Common.setup_3_nodes_mini, Common.basic_teardown)
def test_inject_as_head():
    """
    test_inject_as_head : asserts shortcut for those who don't want to collapse periodically. (eta: 20 s)
    ARAKOON-288
    ARAKOON-308
    """
    cluster = Common._getCluster()
    npt = 1000
    n = 4 * npt + 65
    Common.iterate_n_times(n,Common.simple_set)
    client = Common.get_client()
    m = client.whoMaster()
    slaves = filter(lambda x:x != m, Common.node_names)
    s0 = slaves[0]
    s1 = slaves[1]
    new_head = '/tmp/test_inject_as_head.db'
    print "1"
    cluster.backupDb(s0, new_head)
    logging.info("backup-ed %s from %s", new_head, s0)
    cluster.injectAsHead(s1, new_head)
    print "2"
    logging.info("injected as head")
    Common.iterate_n_times(n,Common.simple_set)
    logging.info("iterated")
    print "3"
    cluster.remoteCollapse(s1, 3)
    logging.info("done")
    print "4"
    ntlogs = Common.get_tlog_count(s1)
    logging.info("get_tlog_dir => %i", ntlogs)

@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_concurrent_collapse_fails():
    """ assert only one collapse goes through at any time (eta : 450s) """
    zero = Common.node_names[0]
    n = 298765
    logging.info("going to do %i sets to fill tlogs", n)
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("Did %i sets, now going into collapse scenario", n)

    class SecondCollapseThread(Thread):
        def __init__(self, sleep_time):
            Thread.__init__(self)

            self.sleep_time = sleep_time
            self.exception_received = False

        def run(self):
            logging.info('Second collapser thread started, sleeping...')
            time.sleep(self.sleep_time)

            logging.info('Starting concurrent collapse')
            rc = Common.collapse(zero, 1)

            logging.info('Concurrent collapse returned %d', rc)

            if rc == 255:
                self.exception_received = True

    s = SecondCollapseThread(8)
    assert not s.exception_received

    logging.info('Launching second collapser thread')
    s.start()

    logging.info('Launching main collapse')
    Common.collapse(zero, 1)

    logging.info("collapsing finished")
    assert_true(s.exception_received)


@Common.with_custom_setup(Common.setup_2_nodes_forced_master, Common.basic_teardown)
def test_catchup_exercises():
    time.sleep(Common.lease_duration) # ??

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

    n = 20000
    w = 80 # 250/s should be more than enough even for dss driven vmachines
    for i in range(5):
        do_one(n,w)
        n = n * 2
        w = w * 2


@Common.with_custom_setup(Common.setup_2_nodes, Common.basic_teardown)
def test_collapse():
    zero = Common.node_names[0]
    one = Common.node_names[1]
    n = 298765
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    Common.collapse(zero,1)
    logging.info("collapsing done")
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
