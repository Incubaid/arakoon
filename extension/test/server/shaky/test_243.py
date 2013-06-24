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
