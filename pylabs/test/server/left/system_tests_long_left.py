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
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
"""

from .. import system_tests_common as C

from nose.tools import *

import time
import subprocess
import threading

from Compat import X

CONFIG = C.CONFIG
def _getCluster():
    cluster_id = CONFIG.cluster_id
    return C._getCluster(cluster_id)

@C.with_custom_setup(C.setup_2_nodes, C.basic_teardown)
def test_collapse():
    zero = CONFIG.node_names[0]
    one = CONFIG.node_names[1]
    n = 298765
    C.iterate_n_times(n, C.simple_set)
    X.logging.info("did %i sets, now going into collapse scenario" % n)
    C.collapse(zero,1)
    X.logging.info("collapsing done")
    C.stopOne(one)
    C.whipe(one)
    C.startOne(one)
    cli = C.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    X.logging.info("catchup from collapsed node finished")

@C.with_custom_setup(C.setup_1_node, C.basic_teardown)
def test_concurrent_collapse_fails():
    zero = CONFIG.node_names[0]
    n = 298765
    C.iterate_n_times(n, C.simple_set)
    X.logging.info("Did %i sets, now going into collapse scenario", n)
    
    class SecondCollapseThread(threading.Thread):
        def __init__(self, sleep_time):
            threading.Thread.__init__(self)
            
            self.sleep_time = sleep_time
            self.exception_received = False
    
        def run(self):
            X.logging.info('Second collapser thread started, sleeping...')
            time.sleep(self.sleep_time)
            
            X.logging.info('Starting concurrent collapse')
            rc = C.collapse(zero, 1)
   
            X.logging.info('Concurrent collapse returned %d', rc)

            if rc == 255:
                self.exception_received = True

    s = SecondCollapseThread(5)
    assert not s.exception_received
       
    X.logging.info('Launching second collapser thread')
    s.start()

    X.logging.info('Launching main collapse')
    C.collapse(zero, 1)
       
    X.logging.info("collapsing finished")
    assert_true(s.exception_received)



@C.with_custom_setup(C.setup_2_nodes_forced_master, C.basic_teardown)
def test_catchup_exercises():
    time.sleep(1.0) # ??
    
    def do_one(n, max_wait):
        C.iterate_n_times(n, C.simple_set)
        C.stop_all()
        X.logging.info("stopped all nodes")
        C.whipe(CONFIG.node_names[1])

        C.start_all()
        cli = C.get_client ()
        counter = 0
        up2date = False

        while not up2date and counter < max_wait :
            time.sleep( 1.0 )
            counter += 1
            up2date = cli.expectProgressPossible()
            X.logging.info("up2date=%s", up2date)
    
        if counter >= max_wait :
            raise Exception ("Node did not catchup in a timely fashion")

    n = 20000
    w = 40 # 500/s should be more than enough even for dss driven vmachines
    for i in range(5):
        do_one(n,w)
        n = n * 2
        w = w * 2


@C.with_custom_setup(C.setup_2_nodes_forced_master, C.basic_teardown)
def test_catchup_only():
    C.iterate_n_times(123000,C.simple_set)
    n0 = CONFIG.node_names[0]
    n1 = CONFIG.node_names[1]
    C.stopOne(n1)
    C.whipe(n1)
    X.logging.info("catchup-only")
    C.catchupOnly(n1)
    X.logging.info("done with catchup-only")
    C.stopOne(n0)
    C.compare_stores(n1,n0)
