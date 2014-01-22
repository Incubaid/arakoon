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

import os.path

from .. import system_tests_common as Common
from arakoon.ArakoonExceptions import *
import arakoon
import logging
import time
import subprocess
from threading import Thread
from nose.tools import *


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
    ret = cluster.injectAsHead(s1, new_head)

    if ret != 0:
        raise RuntimeError('injectAsHead returned %d' % ret)

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

@Common.with_custom_setup(Common.setup_3_nodes_mini, Common.basic_teardown)
def test_inplace_inject_as_head():
    cluster = Common._getCluster()
    npt = 1000
    n = 4 * npt + 65
    Common.iterate_n_times(n,Common.simple_set)
    client = Common.get_client()
    m = client.whoMaster()
    slaves = filter(lambda x:x != m, Common.node_names)
    s0 = slaves[0]
    s1 = slaves[1]

    config = cluster._getConfigFile()
    def get_path(c, n, e):
        if len(e) == 0:
            return None

        f, r = e[0], e[1:]
        if c.checkParam(n, f):
            return c.getValue(n, f)
        else:
            return get_path(c, n, r)

    s1_head_dir = get_path(config, s1,
        ['head_dir', 'tlf_dir', 'tlog_dir', 'home'])
    if not s1_head_dir:
        raise Exception('Unable to find suitable head dir')

    new_head = os.path.join(s1_head_dir, 'test_inject_as_head.db')

    print "1"
    cluster.backupDb(s0, new_head)
    logging.info("backup-ed %s from %s", new_head, s0)
    ret = cluster.injectAsHead(s1, new_head, inPlace=True)

    if ret != 0:
        raise RuntimeError('injectAsHead returned %d' % ret)

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


@Common.with_custom_setup(Common.setup_3_nodes_mini_forced_master, Common.basic_teardown)
def test_inject_as_head_witness_node():
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
    s0 = Common.node_names[2] # this is a normal slave from which you can do backupDb
    s1 = Common.node_names[1] # this is a forced slave which should support inject_as_head
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

@Common.with_custom_setup(Common.setup_3_nodes_mini, Common.basic_teardown)
def test_inject_as_head_failure():
    '''Test inject_as_head with a fake database file (should fail)'''

    logging.info('Filling cluster')
    cluster = Common._getCluster()
    Common.iterate_n_times(10, Common.simple_set)

    logging.info('Looking up master')
    client = Common.get_client()
    m = client.whoMaster()
    slaves = filter(lambda x: x != m, Common.node_names)
    s = slaves[0]

    logging.info('Creating broken database')
    new_head = '/tmp/broken.db'
    with open(new_head, 'w') as fd:
        fd.write('this is not a TC database')

    logging.info('Injecting broken database')
    ret = cluster.injectAsHead(s, new_head)

    if ret != 4:
        raise RuntimeError('Unexpected injectAsHead result: %r', ret)

@Common.with_custom_setup(Common.setup_3_nodes_mini, Common.basic_teardown)
def test_inject_as_head_current_corrupt():

    logging.info('Filling cluster')
    cluster = Common._getCluster()
    Common.iterate_n_times(10, Common.simple_set)

    logging.info('Looking up master')
    client = Common.get_client()
    m = client.whoMaster()
    slaves = filter(lambda x: x != m, Common.node_names)
    s = slaves[0]

    good_head = '/tmp/h.db'
    cluster.backupDb(s, good_head)

    Common.stopOne(s)
    current_head = Common.getConfig(s)['head_dir'] + '/head.db'
    logging.info('Creating broken head database for slave at %s', current_head)
    with open(current_head, 'w') as fd:
        fd.write('this is not a TC database')

    Common.startOne(s)
    time.sleep(1.0)

    logging.info('Injecting good head without --force')
    ret = cluster.injectAsHead(s, good_head)

    if ret != 3:
        raise RuntimeError('Unexpected injectAsHead result: %r', ret)

    logging.info('Injecting good head with --force')
    ret = cluster.injectAsHead(s, good_head, force = True)

    if not (ret in [0, None]):
        raise RuntimeError('Unexpected injectAsHead --force result: %r', ret)
