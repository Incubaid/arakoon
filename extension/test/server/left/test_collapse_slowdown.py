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


@Common.with_custom_setup(Common.setup_3_nodes_forced_master_slow_collapser, Common.basic_teardown)
def test_collapse_slowdown():
    """ Test wether collapse_slowdown really slows stuff down """
    slow_slave = Common.node_names[1]
    fast_slave = Common.node_names[2]
    n = 298765
    logging.info("going to do %i sets to fill tlogs", n)
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("Did %i sets, now going into collapse scenario", n)

    t0 = time.time()
    logging.info('Collapsing slow slave')
    rc = Common.collapse(slow_slave, 1)
    assert rc == 0
    t1 = time.time()
    slow_duration = t1 - t0
    logging.info('slow collapse took %f', slow_duration)

    t0 = time.time()
    logging.info('Collapsing slow slave')
    rc = Common.collapse(fast_slave, 1)
    assert rc == 0
    t1 = time.time()
    normal_duration = t1 - t0
    logging.info('normal collapse took %f', normal_duration)

    assert 3 * normal_duration < slow_duration < 10 * normal_duration
