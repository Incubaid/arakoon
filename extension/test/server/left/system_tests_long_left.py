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

from ..system_tests_common import with_custom_setup

from ..system_tests_common import setup_1_node, setup_2_nodes
from ..system_tests_common import setup_2_nodes_forced_master
from ..system_tests_common import basic_teardown

from .. import system_tests_common as C

from nose.tools import *

import time
import logging
import subprocess
import threading

def _getCluster():
    global cluster_id
    return q.manage.arakoon.getCluster(cluster_id)

@with_custom_setup(setup_2_nodes, basic_teardown)
def test_collapse():
    zero = C.node_names[0]
    one = C.node_names[1]
    n = 298765
    C.iterate_n_times(n, C.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    C.collapse(zero,1)
    logging.info("collapsing done")
    C.stopOne(one)
    C.wipe(one)
    C.startOne(one)
    cli = C.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")

@with_custom_setup(setup_1_node, basic_teardown)
def test_concurrent_collapse_fails():
    """ assert only one collapse goes through at any time (eta : 450s) """
    zero = C.node_names[0]
    n = 298765
    C.iterate_n_times(n, C.simple_set)
    logging.info("Did %i sets, now going into collapse scenario", n)
    
    class SecondCollapseThread(threading.Thread):
        def __init__(self, sleep_time):
            threading.Thread.__init__(self)
            
            self.sleep_time = sleep_time
            self.exception_received = False
    
        def run(self):
            logging.info('Second collapser thread started, sleeping...')
            time.sleep(self.sleep_time)
            
            logging.info('Starting concurrent collapse')
            rc = C.collapse(zero, 1)
   
            logging.info('Concurrent collapse returned %d', rc)

            if rc == 255:
                self.exception_received = True

    s = SecondCollapseThread(5)
    assert not s.exception_received
       
    logging.info('Launching second collapser thread')
    s.start()

    logging.info('Launching main collapse')
    C.collapse(zero, 1)
       
    logging.info("collapsing finished")
    assert_true(s.exception_received)



@with_custom_setup(setup_2_nodes_forced_master, basic_teardown)
def test_catchup_exercises():
    time.sleep(C.lease_duration) # ??
    
    def do_one(n, max_wait):
        logging.info("do_one(%i,%f)", n, max_wait)
        C.iterate_n_times(n, C.simple_set)
        logging.info("sets done, stopping")
        C.stop_all()
        logging.info("stopped all nodes")
        nn = C.node_names[1]
        C.wipe(nn)

        C.start_all()
        logging.info("started all")
        
        counter = 0
        up2date = False
        cli = C.get_client ()
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


@with_custom_setup(setup_2_nodes_forced_master, basic_teardown)
def test_catchup_only():
    C.iterate_n_times(123000, C.simple_set)
    n0 = C.node_names[0]
    n1 = C.node_names[1]
    logging.info("stopping %s", n1)
    C.stopOne(n1)
    C.wipe(n1)
    logging.info("catchup-only")
    C.catchupOnly(n1)
    logging.info("done with catchup-only")
    C.stopOne(n0)
    C.compare_stores(n1,n0)
