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
import nose.tools as NT
import time
import logging

@Common.with_custom_setup(Common.setup_3_nodes, Common.basic_teardown)
def test_prefered_master():
    cluster = q.manage.arakoon.getCluster(cluster_id)
    cluster.stop()
    pm = Common.node_names[0]
    cluster.forceMaster(pm,preferred=True)
    cluster.start()
    Common.assert_running_nodes(3)
    time.sleep(4.0 * lease_duration)
    logging.info("all should be well")
    client = Common.get_client()
    master = client.whoMaster()
    NT.assert_equals(pm, master)
    cluster.stopOne(pm)
    time.sleep(8.0 * lease_duration)
    client = Common.get_client()
    master2 = client.whoMaster()
    logging.info("master2 = %s",master2)
    NT.assert_equals(True, master2 != pm)
    cluster.startOne(pm)
    time.sleep(4.0* lease_duration)
    client = Common.get_client()
    master3 = client.whoMaster()
    logging.info("master3 = %s", master3)
    NT.assert_equals(master3,pm)
    
