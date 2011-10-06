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

from .. import system_tests_common as C
from arakoon.ArakoonExceptions import *
import arakoon
import time
from nose.tools import *
import logging
from arakoon.NurseryRouting import RoutingInfo, LeafRoutingNode

def validate_keys_in_nursery (n_cli, keys ):
    for k in keys:
        stored = n_cli.get(k)
        assert_equals( stored , k, "Key has wrong value '%s' != '%s' (stored != expected)" % (stored, k) )


def multi_migration_scenario( migrations, keys, final_routing):
    
    cli = C.get_nursery_client()
    for k in keys :
        cli.set(k , k)
    
    n = C.get_nursery()
    for m in migrations:
        left, sep, right = m
        n.migrate( left, sep, right )
    
    ara_clients = dict()
    for k in keys:
        clu_id = final_routing.getClusterId(k)
        if not ara_clients.has_key(clu_id):
            ara_clients[clu_id] = C.get_client( clu_id )
        ara_cli = ara_clients[clu_id]
        assert_equals(ara_cli.get(k), k, "Key not migrated properly %s" % k)
    
    for (clu_id, ara_cli) in ara_clients.iteritems():
        ara_cli._dropConnections()
        
    ara_clients.clear()
    
    validate_keys_in_nursery(cli,keys)
    n_cli = C.get_nursery_client()
    validate_keys_in_nursery(n_cli, keys)
            
@C.with_custom_setup( C.setup_nursery_2, C.nursery_teardown )
def test_nursery_single_migration_1():
    keys = ['a', 'b', 'l', 'm']
    clus = C.nursery_cluster_ids     
    migrations = [ (clus[0], 'k', clus[1]) ]
    routing = RoutingInfo(LeafRoutingNode(clus[0]))
    routing.split('k', clus[1])
    multi_migration_scenario(migrations, keys, routing)
    
@C.with_custom_setup( C.setup_nursery_2, C.nursery_teardown )
def test_nursery_single_migration_2():
    keys = ['a', 'b', 'l', 'm']
    clus = C.nursery_cluster_ids     
    migrations = [ (clus[1], 'k', clus[0]) ]
    routing = RoutingInfo(LeafRoutingNode(clus[1]))
    routing.split('k', clus[0])
    multi_migration_scenario(migrations, keys, routing)
    
@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_invalid_migration():
    # Give clusters some time to register themselves with the keeper
    time.sleep(5.0)
    cli = C.get_nursery_client()
    n = C.get_nursery()
    assert_raises( RuntimeError, n.migrate, C.nursery_cluster_ids[1], "k", C.nursery_cluster_ids[2] )
    assert_raises( RuntimeError, n.migrate, "none_0", "k", "none_1" )
    n.migrate( C.nursery_cluster_ids[0], "k", C.nursery_cluster_ids[1])
    assert_raises( RuntimeError, n.migrate, C.nursery_cluster_ids[0], "l", C.nursery_cluster_ids[2] )


def migration_scenario_1( migrations ):
    keys = ['a', 'b', 'l', 'm', 'y', 'z']
    clus = C.nursery_cluster_ids
    routing = RoutingInfo( LeafRoutingNode(clus[0]) )
    routing.split("d", clus[1])
    routing.split("p", clus[2])
    multi_migration_scenario(migrations, keys, routing)
    
@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_double_migration_1():
    clus = C.nursery_cluster_ids
    migrations = [
        (clus[0], 'p', clus[2]),
        (clus[0], 'd', clus[1])
    ]
    migration_scenario_1(migrations)

@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_double_migration_2():
    clus = C.nursery_cluster_ids
    migrations = [
        (clus[0], 'd', clus[2]),   
        (clus[1], 'p', clus[2])
    ]
    migration_scenario_1(migrations)

@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_triple_migration_1():
    clus = C.nursery_cluster_ids
    migrations = [
        (clus[0], 'd', clus[2]),   
        (clus[1], 'k', clus[2]),
        (clus[1], 'p', clus[2])
    ]
    migration_scenario_1(migrations)

@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_triple_migration_2():
    clus = C.nursery_cluster_ids
    migrations = [
        (clus[0], 'd', clus[2]),   
        (clus[1], 'z', clus[2]),
        (clus[1], 'p', clus[2])
    ]
    migration_scenario_1(migrations)
    

@C.with_custom_setup( C.setup_nursery_2, C.nursery_teardown )
def test_nursery_multi_phaze_migration_1():
    clus = C.nursery_cluster_ids
    keys = []
    for i in range(2048):
        keys.append( "l_%0512d" % (i) )
    migrations = [
        (clus[0], 'k', clus[1])
    ]    
    routing = RoutingInfo( LeafRoutingNode(clus[0]) )
    routing.split("d", clus[1])
    multi_migration_scenario(migrations, keys, routing)

