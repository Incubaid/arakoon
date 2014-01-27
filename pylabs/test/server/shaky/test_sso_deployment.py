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

from .. import system_tests_common as Common
from arakoon.ArakoonExceptions import *
import arakoon
import logging
import time
import subprocess
from nose.tools import *
import os
import random
from threading import Thread, Condition
from Compat import X

@Common.with_custom_setup( Common.setup_1_node_forced_master, Common.basic_teardown )
def test_sso_deployment():
    """ the scaling scenario from 1 node to 3 nodes the way A-server does it (eta : 2200 s) """
    global test_failed
    test_failed = False

    def write_loop ():
        Common.iterate_n_times( 10000,
                                Common.retrying_set_get_and_delete )

    def large_write_loop ():
        Common.iterate_n_times( 280000,
                                Common.retrying_set_get_and_delete,
                                startSuffix = 1000000 )

    write_thr1 = Common.create_and_start_thread ( write_loop )
    def non_retrying_write_loop ():
        Common.iterate_n_times( 10000,
                                Common.set_get_and_delete,
                                startSuffix = 2000000  )

    Common.add_node( 1 )
    cl = Common._getCluster()
    cl.setLogLevel("debug")

    Common.regenerateClientConfig(Common.cluster_id)

    Common.restart_nodes_wf_sim( 1 )
    n1 = Common.node_names[1]
    logging.info("going to start %s", n1)
    Common.startOne(n1 )

    Common.create_and_wait_for_thread_list ( [ large_write_loop ] )

    Common.add_node( 2 )
    cl = Common._getCluster()
    cl.setLogLevel("debug")
    cl.forceMaster(None )
    logging.info("2 node config without forced master")

    Common.regenerateClientConfig(Common.cluster_id)

    Common.restart_nodes_wf_sim( 2 )
    Common.startOne( Common.node_names[2] )
    time.sleep( 0.3 )
    Common.assert_running_nodes ( 3 )

    write_thr3 = Common.create_and_start_thread ( non_retrying_write_loop )

    write_thr1.join()
    write_thr3.join()

    assert_false ( test_failed )

    Common.assert_running_nodes( 3 )
