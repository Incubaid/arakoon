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
from ..system_tests_common import node_names
from ..system_tests_common import iterate_n_times
from ..system_tests_common import simple_set
from ..system_tests_common import collapse

import logging
import subprocess

def _getCluster():
    global cluster_id
    return q.manage.arakoon.getCluster(cluster_id)

@with_custom_setup(setup_2_nodes, basic_teardown)
def test_collapse():
    zero = node_names[0]
    one = node_names[1]
    n = 298765
    iterate_n_times(n, simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    collapse(zero,1)
    logging.info("collapsing done")
    stopOne(one)
    whipe(one)
    startOne(one)
    cli = get_client()
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
    zero = node_names[0]
    n = 298765
    iterate_n_times(n, simple_set)
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
            rc = collapse(zero, 1)
   
            logging.info('Concurrent collapse returned %d', rc)

            if rc == 255:
                self.exception_received = True

    s = SecondCollapseThread(5)
    assert not s.exception_received
       
    logging.info('Launching second collapser thread')
    s.start()

    logging.info('Launching main collapse')
    collapse(zero, 1)
       
    logging.info("collapsing finished")
    assert_true(s.exception_received)



@with_custom_setup(setup_2_nodes_forced_master, basic_teardown)
def test_catchup_exercises():
    time.sleep(1.0) # ??
    
    def do_one(n, max_wait):
        iterate_n_times(n, simple_set)
        stop_all()
        logging.info("stopped all nodes")
        whipe(node_names[1])

        start_all()
        cli = get_client ()
        counter = 0
        up2date = False

        while not up2date and counter < max_wait :
            time.sleep( 1.0 )
            counter += 1
            up2date = cli.expectProgressPossible()
            logging.info("up2date=%s", up2date)
    
        if counter >= max_wait :
            raise Exception ("Node did not catchup in a timely fashion")

    n = 20000
    w = 40 # 500/s should be more than enough even for dss driven vmachines
    for i in range(5):
        do_one(n,w)
        n = n * 2
        w = w * 2


@with_custom_setup(setup_2_nodes_forced_master, basic_teardown)
def test_catchup_only():
    iterate_n_times(123000,simple_set)
    n1 = node_names[1]
    stopOne(n1)
    whipe(n1)
    logging.info("catchup-only")
    catchupOnly(n1)
    logging.info("done with catchup-only")
    startOne(n1)
    cli = get_client()
    time.sleep(1.0)
    up2date = cli.expectProgressPossible()
    assert_true(up2date)
