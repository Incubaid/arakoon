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
def test_272():
    """
    test_272 : arakoon can go down during log rotation, but you need to have load to reproduce it (eta: 1110s)
    """
    node = Common.node_names[0]
    cluster = Common._getCluster()
    path = '%s.cfg' % cluster._getConfigFilePath()
    logging.info('path=%s', path)
    bench = subprocess.Popen([Common.binary_full_path, '-config', path ,'--benchmark',
                              '-max_n', '100000'])
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
