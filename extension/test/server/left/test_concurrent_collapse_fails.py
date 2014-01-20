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


@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_concurrent_collapse_fails():
    """ assert only one collapse goes through at any time (eta : 450s) """
    zero = Common.node_names[0]
    Common.stopOne(zero)
    Common._getCluster().setCollapseSlowdown(0.3)
    Common.startOne(zero)
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

    s = SecondCollapseThread(1)
    assert not s.exception_received

    logging.info('Launching second collapser thread')
    s.start()

    logging.info('Launching main collapse')
    Common.collapse(zero, 1)

    logging.info("collapsing finished")
    assert_true(s.exception_received)
