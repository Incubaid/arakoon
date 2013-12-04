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
from nose.tools import assert_true
import time
import logging

@Common.with_custom_setup(Common.setup_2_nodes, Common.basic_teardown)
def test_learner():
    op_count = 54321
    Common.iterate_n_times(op_count, Common.simple_set)
    cluster = Common.q.manage.arakoon.getCluster(Common.cluster_id)
    logging.info("adding learner")
    name = Common.node_names[2]
    (db_dir, log_dir, tlf_dir, head_dir) = Common.build_node_dir_names(name)
    cluster.addNode(name,
                    Common.node_ips[2],
                    clientPort = Common.node_client_base_port + 2,
                    messagingPort = Common.node_msg_base_port + 2,
                    logDir = log_dir,
                    tlfDir = tlf_dir,
                    headDir = head_dir,
                    logLevel = 'debug',
                    home = db_dir,
                    isLearner = True,
                    targets = [Common.node_names[0]])
    cluster.disableFsync([name])
    cluster.addLocalNode(name)
    cluster.createDirs(name)
    cluster.startOne(name)

    time.sleep(1.0)

    Common.assert_running_nodes(3)
    time.sleep(op_count / 1000 + 1 ) # 1000/s in catchup should be no problem
    #use a client ??"
    Common.stop_all()
    i2 = int(Common.get_last_i_tlog(name))
    assert_true(i2 >= op_count - 1)
