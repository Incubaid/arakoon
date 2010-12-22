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


import system_tests_common as Common
from pymonkey import q
import logging

def last_slave(master_id):
    slaves = filter(lambda x: x!= master_id, Common.node_names)
    return slaves [-1]

def start_node(node_id):
    q.cmdtools.arakoon.startOne(node_id)

def stop_node(node_id):
    q.cmdtools.arakoon.stopOne(node_id)
    
@Common.with_custom_setup(Common.setup_3_nodes, Common.basic_teardown)
def test_shaky_slave():
    cli = Common.get_client()
    master_id = cli.whoMaster()
    slave_id = last_slave(master_id)
    stop_node(slave_id)
    print ("slave %s stopped" % slave_id)
    n = 5000
    Common.iterate_n_times( n, Common.simple_set)
    for i in range(100):
        print ("starting cycle %i" % i)
        start_node(slave_id)
        Common.iterate_n_times( n, Common.simple_set)
        stop_node(slave_id)
        Common.iterate_n_times( n, Common.simple_set)
    print "phewy!"
    
