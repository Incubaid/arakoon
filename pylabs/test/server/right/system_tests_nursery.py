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
from arakoon.NurseryRouting import RoutingInfo, LeafRoutingNode

from Compat import X
#import logging

CLUSTER_IDS = C.CONFIG.nursery_cluster_ids

def validate_keys_in_nursery (n_cli, keys ):
    X.logging.debug("validate_keys_in_nursery %s", keys)
    n_cli._fetchNurseryConfig()
    r = n_cli._routing
    for k in keys:
        cid = r.getClusterId(k)
        exists = n_cli.exists(k)
        X.logging.debug("%s => %s exists:%s", k, cid, exists)
        if not exists :
            assert_true(False, "'%s' should exist in nursery" % k)
        stored = n_cli.get(k)
        assert_equals( stored , k, "Key has wrong value '%s' != '%s' (stored != expected)" % (stored, k) )


def check_migrated_keys(keys, final_routing):
    ara_clients = dict()
    for k in keys:
        clu_id = final_routing.getClusterId(k)
        X.logging.debug("key %s => cluster: %s", k, clu_id)
        if not ara_clients.has_key(clu_id):
            X.logging.debug("getting new client for cluster '%s'", clu_id)
            ara_cli = C.get_client(protocol_version = 2, c_id = clu_id )
            ara_clients[clu_id] = ara_cli

        ara_cli = ara_clients[clu_id]
        has_it = ara_cli.exists(k)
        assert_equals(has_it, True, "Key not migrated properly '%s' it should be in cluster %s" % (k, clu_id))
    
    for (clu_id, ara_cli) in ara_clients.iteritems():
        ara_cli.dropConnections()

        
    
def multi_migration_scenario( migrations, keys, final_routing):
    
    cli = C.get_nursery_client()
    for k in keys :
        cli.set(k , k)
    
    n = C.get_nursery()
    for m in migrations:
        left, sep, right = m
        X.logging.debug("going to migrate: %s, %s ,%s", left,sep,right)
        n.migrate( left, sep, right )
        cli._fetchNurseryConfig()
        r = cli._routing 
        assert_true(r.contains(left), "Routing not correctly updated. Should contain %s" % left )
        assert_true(r.contains(right), "Routing not correctly updated. Should contain %s" % right )
    
    check_migrated_keys(keys, final_routing)
    X.logging.debug("migration checked")
    n_cli = C.get_nursery_client()
    X.logging.debug("class:%s", n_cli.__class__)
    validate_keys_in_nursery(n_cli, keys)
            
@C.with_custom_setup( C.setup_nursery_2, C.nursery_teardown )
def test_nursery_single_migration_1():
    keys = ['a', 'b', 'l', 'm']
    migrations = [ (CLUSTER_IDS[0], 'k', CLUSTER_IDS[1]) ]
    routing = RoutingInfo(LeafRoutingNode(CLUSTER_IDS[0]))
    routing.split('k', CLUSTER_IDS[1])
    multi_migration_scenario(migrations, keys, routing)
    
@C.with_custom_setup( C.setup_nursery_2, C.nursery_teardown )
def test_nursery_single_migration_2():
    keys = ['a', 'b', 'l', 'm']
    migrations = [ (CLUSTER_IDS[1], 'k', CLUSTER_IDS[0]) ]
    routing = RoutingInfo(LeafRoutingNode(CLUSTER_IDS[1]))
    routing.split('k', CLUSTER_IDS[0])
    multi_migration_scenario(migrations, keys, routing)
    
@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_invalid_migration():

    cli = C.get_nursery_client()
    n = C.get_nursery()
    assert_raises( RuntimeError, n.migrate, CLUSTER_IDS[1], "k", CLUSTER_IDS[2] )
    assert_raises( RuntimeError, n.migrate, "none_0", "k", "none_1" )
    n.migrate( CLUSTER_IDS[0], "k", CLUSTER_IDS[1])
    assert_raises( RuntimeError, n.migrate, CLUSTER_IDS[0], "l", CLUSTER_IDS[2] )


def migration_scenario_1( migrations ):
    keys = ['a', 'b', 'l', 'm', 'y', 'z']
    routing = RoutingInfo( LeafRoutingNode(CLUSTER_IDS[0]) )
    routing.split("d", CLUSTER_IDS[1])
    routing.split("p", CLUSTER_IDS[2])
    multi_migration_scenario(migrations, keys, routing)
    
@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_double_migration_1():
    migrations = [
        (CLUSTER_IDS[0], 'p', CLUSTER_IDS[2]),
        (CLUSTER_IDS[0], 'd', CLUSTER_IDS[1])
    ]
    migration_scenario_1(migrations)

@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_double_migration_2():
    migrations = [
        (CLUSTER_IDS[0], 'd', CLUSTER_IDS[2]),   
        (CLUSTER_IDS[1], 'p', CLUSTER_IDS[2])
    ]
    migration_scenario_1(migrations)

@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_triple_migration_1():
    migrations = [
        (CLUSTER_IDS[0], 'd', CLUSTER_IDS[2]),   
        (CLUSTER_IDS[1], 'k', CLUSTER_IDS[2]),
        (CLUSTER_IDS[1], 'p', CLUSTER_IDS[2])
    ]
    migration_scenario_1(migrations)

@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_triple_migration_2():
    migrations = [
        (CLUSTER_IDS[0], 'd', CLUSTER_IDS[2]),   
        (CLUSTER_IDS[1], 'z', CLUSTER_IDS[2]),
        (CLUSTER_IDS[1], 'p', CLUSTER_IDS[2])
    ]
    migration_scenario_1(migrations)
    

@C.with_custom_setup( C.setup_nursery_2, C.nursery_teardown )
def test_nursery_multi_phaze_migration_1():
    keys = []
    for i in range(2048):
        keys.append( "l_%0512d" % (i) )
    migrations = [
        (CLUSTER_IDS[0], 'k', CLUSTER_IDS[1])
    ]    
    routing = RoutingInfo( LeafRoutingNode(CLUSTER_IDS[0]) )
    routing.split("d", CLUSTER_IDS[1])
    multi_migration_scenario(migrations, keys, routing)

def delete_scenario(to_delete, separator, final_routing):
    
    node_to_delete  = CLUSTER_IDS[to_delete]
    keys = ['a', 'b', 'l', 'm', 'y', 'z']
    migrations = [
        (CLUSTER_IDS[0], 'd', CLUSTER_IDS[1]),
        (CLUSTER_IDS[1], 'p', CLUSTER_IDS[2])
    ]
    routing = RoutingInfo( LeafRoutingNode(CLUSTER_IDS[0]) )
    routing.split("d", CLUSTER_IDS[1])
    routing.split("p", CLUSTER_IDS[2])
    
    multi_migration_scenario(migrations, keys, routing)
    
    n = C.get_nursery()
    n.delete( node_to_delete, separator )
    
    check_migrated_keys( keys, final_routing )
    
    n_cli = C.get_nursery_client()
    assert_false( n_cli._routing.contains( node_to_delete ) , 
                  "Routing still contains cluster %s which should have been deleted - %s" % 
                  (node_to_delete, str(n_cli._routing) ) )
    
@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_delete_scenario_1():
    final_routing = RoutingInfo( LeafRoutingNode(CLUSTER_IDS[1]) )
    final_routing.split("p", CLUSTER_IDS[2])
    delete_scenario( 0, None, final_routing )

@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_delete_scenario_2():
    final_routing = RoutingInfo( LeafRoutingNode(CLUSTER_IDS[0]) )
    final_routing.split("m", CLUSTER_IDS[2])
    delete_scenario( 1, "m", final_routing )
    
@C.with_custom_setup( C.setup_nursery_3, C.nursery_teardown )
def test_nursery_delete_scenario_3():
    final_routing = RoutingInfo( LeafRoutingNode(CLUSTER_IDS[0]) )
    final_routing.split("d", CLUSTER_IDS[1])
    delete_scenario( 2, None, final_routing )

def test_routing_contains():
    routing = RoutingInfo( LeafRoutingNode(CLUSTER_IDS[0]) )
    assert_true(routing.contains(CLUSTER_IDS[0]), "Not even one is correct")
    routing.split("d", CLUSTER_IDS[1])
    routing.split("p", CLUSTER_IDS[2])
    
