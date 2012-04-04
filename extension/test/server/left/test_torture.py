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

import Compat as X

from .. import system_tests_common as C

def last_slave(master_id):
    slaves = filter(lambda x: x!= master_id, C.CONFIG.node_names)
    return slaves [-1]

    
@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_shaky_slave():
    cli = C.get_client()
    master_id = cli.whoMaster()
    slave_id = last_slave(master_id)
    C.stopOne(slave_id)
    print ("slave %s stopped" % slave_id)
    n = 2000
    C.iterate_n_times( n, C.simple_set)
    cycles = 100 
    for i in range(cycles):
        print ("starting cycle %i" % i)
        C.startOne(slave_id)
        C.iterate_n_times( n, C.simple_set)
        C.stopOne(slave_id)
        C.iterate_n_times( n, C.simple_set)
    print "phewy!"

@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_fat_shaky_slave():
    cli = C.get_client()
    master_id = cli.whoMaster()
    slave_id = last_slave(master_id)
    C.stopOne(slave_id)
    print ("slave %s stopped" % slave_id)
    n = 20000
    C.iterate_n_times( n, C.simple_set)
    cycles = 10 
    for i in range(cycles):
        print ("starting cycle %i" % i)
        C.startOne(slave_id)
        C.iterate_n_times( n, C.simple_set)
        C.stopOne(slave_id)
        C.iterate_n_times( n, C.simple_set)
    print "phewy!"

@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_shaky_cluster():
    n = 500
    names = C.CONFIG.node_names
    def stop_all():
        for node_name in names:
            C.stopOne(node_name)
    
    def start_all():
        for node_name in names:
            C.startOne(node_name)
    
    for i in range(n):
        C.restart_all()
        C.assert_running_nodes(3)
    
        
