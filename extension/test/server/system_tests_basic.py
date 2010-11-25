'''
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
'''

from system_tests_common import *
import logging

@with_custom_setup ( setup_1_node_forced_master, basic_teardown )
def test_start_stop_single_node_forced () :
    assert_running_nodes ( 1 )
    q.cmdtools.arakoon.stop() 
    assert_running_nodes ( 0 )
    q.cmdtools.arakoon.start() 
    assert_running_nodes ( 1 )

@with_custom_setup ( setup_3_nodes_forced_master, basic_teardown )
def test_start_stop_three_nodes_forced () :
    assert_running_nodes ( 3 )
    q.cmdtools.arakoon.stop() 
    assert_running_nodes ( 0 )
    q.cmdtools.arakoon.start() 
    assert_running_nodes ( 3 )

@with_custom_setup( default_setup, basic_teardown )        
def test_single_client_100_set_get_and_deletes() :
    iterate_n_times( 100, set_get_and_delete )
    
@with_custom_setup( setup_1_node_forced_master, basic_teardown)
def test_deploy_1_to_2():
    add_node_scenario (1)
    
@with_custom_setup( setup_2_nodes_forced_master, basic_teardown)
def test_deploy_2_to_3():
    add_node_scenario (2)

@with_custom_setup( default_setup, basic_teardown )
def test_range ():
    range_scenario ( 1000 )
        
def range_scenario ( start_suffix ):

    iterate_n_times( 100, simple_set, startSuffix = start_suffix )
    
    client = get_client()
    
    start_key = key_format_str % (start_suffix )
    end_key = key_format_str % (start_suffix + 100 )
    test_key = key_format_str % (start_suffix + 25)
    test_key_2 = key_format_str % (start_suffix + 50)
    
    key_list = client.range( test_key , True, end_key , False )
    assert_key_list ( start_suffix+25, 75, key_list )
    
    key_list = client.range( test_key , False, end_key , False )
    assert_key_list ( start_suffix+26, 74, key_list )
    
    key_list = client.range( test_key, True, end_key , False, 10 )
    assert_key_list ( start_suffix+25, 10, key_list )
    
    key_list = client.range( start_key, True, test_key , False )
    assert_key_list ( start_suffix, 25, key_list)
    
    key_list = client.range( start_key, True, test_key , True )
    assert_key_list ( start_suffix, 26, key_list)
    
    key_list = client.range( start_key, True, test_key , False, 10 )
    assert_key_list ( start_suffix, 10, key_list )
    
    key_list = client.range( test_key, True, test_key_2 , False )
    assert_key_list ( start_suffix+25, 25, key_list )
    
    key_list = client.range( test_key, False, test_key_2 , True )
    assert_key_list ( start_suffix+26, 25, key_list )
    
    key_list = client.range( test_key, True, test_key_2 , False, 10 )
    assert_key_list ( start_suffix+25, 10, key_list )
    

@with_custom_setup( default_setup, basic_teardown )
def test_range_entries ():
    range_entries_scenario( 1000 )
    
def range_entries_scenario( start_suffix ):
    
    iterate_n_times( 100, simple_set, startSuffix = start_suffix )
    
    client = get_client()
    
    start_key = key_format_str % (start_suffix )
    end_suffix = key_format_str % ( start_suffix + 100 )
    test_key = key_format_str % (start_suffix + 25)
    test_key_2 = key_format_str % (start_suffix + 50)
    
    key_value_list = client.range_entries ( test_key , True, end_suffix , False )
    assert_key_value_list ( start_suffix + 25, 75, key_value_list )
    
    key_value_list = client.range_entries( test_key , False, end_suffix , False )
    assert_key_value_list ( start_suffix + 26, 74, key_value_list )
    
    key_value_list = client.range_entries( test_key, True, end_suffix , False, 10 )
    assert_key_value_list ( start_suffix + 25, 10, key_value_list )
    
    key_value_list = client.range_entries( start_key, True, test_key , False )
    assert_key_value_list ( start_suffix, 25, key_value_list)
    
    key_value_list = client.range_entries( start_key, True, test_key , True )
    assert_key_value_list ( start_suffix, 26, key_value_list)

    key_value_list = client.range_entries( start_key, True, test_key , False, 10 )
    assert_key_value_list ( start_suffix, 10, key_value_list )
    
    key_value_list = client.range_entries( test_key, True, test_key_2 , False )
    assert_key_value_list ( start_suffix + 25, 25, key_value_list )
    
    key_value_list = client.range_entries( test_key, False, test_key_2 , True )
    assert_key_value_list ( start_suffix + 26, 25, key_value_list )
    
    key_value_list = client.range_entries( test_key, True, test_key_2 , False, 10 )
    assert_key_value_list ( start_suffix + 25, 10, key_value_list )
    
def prefix_scenario( start_suffix ):
    iterate_n_times( 100, simple_set, startSuffix = start_suffix )
    
    test_key_pref = key_format_str  % ( start_suffix + 90 ) 
    test_key_pref = test_key_pref [:-1]
    
    client = get_client()
    
    key_list = client.prefix( test_key_pref )
    assert_key_list ( start_suffix + 90, 10, key_list)
    
    key_list = client.prefix( test_key_pref, 7 )
    assert_key_list ( start_suffix + 90, 7, key_list)
    
    client._dropConnections ()

@with_custom_setup( default_setup, basic_teardown )
def test_prefix ():
    prefix_scenario(1000)

def tes_and_set_scenario( start_suffix ):
    client = get_client()
    
    old_value_prefix = "old_"
    new_value_prefix = "new_"
    
    for i in range ( 1000 ) :
        
        old_value = old_value_prefix + value_format_str % ( i+start_suffix )
        new_value = new_value_prefix + value_format_str % ( i+start_suffix )
        key = key_format_str % ( i+start_suffix )
    
        client.set( key, old_value )
        set_value = client.testAndSet( key, old_value , new_value )
        assert_equals( set_value, old_value ) 
        
        set_value = client.get ( key )
        assert_equals( set_value, new_value )
    
        set_value = client.testAndSet( key, old_value, old_value )
        assert_equals( set_value, new_value )
    
        set_value = client.get ( key )
        assert_not_equals( set_value, old_value )
        
        client.delete( key )
        assert_raises( ArakoonNotFound, client.get, key )
    
    client._dropConnections()


@with_custom_setup( default_setup, basic_teardown )
def test_test_and_set() :
    tes_and_set_scenario( 100000 )
    
@with_custom_setup( setup_3_nodes_forced_master , basic_teardown )
def test_who_master_fixed () :
    client = get_client()
    node = client.whoMaster()
    assert_equals ( node, node_names[0] ) 
    client._dropConnections()

@with_custom_setup( setup_3_nodes , basic_teardown )
def test_who_master () :
    client = get_client()
    node = client.whoMaster()
    assert_true ( node in node_names ) 
    client._dropConnections()

@with_custom_setup( setup_3_nodes_forced_master, basic_teardown )
def test_restart_single_slave_short ():
    restart_single_slave_scenario( 2, 100 )


def test_daemon_version () :
    cmd = "%s --version" % binary_full_path 
    (exit,stdout,stderr) = q.system.process.run(cmd)
    logging.debug( "STDOUT: \n%s" % stdout )
    logging.debug( "STDERR: \n%s" % stderr )
    assert_equals( exit, 0 )
    version = stdout.split('"') [1]
    assert_not_equals( "000000000000", version, "Invalid version 000000000000" )
    local_mods = version.find ("+") 
    assert_equals( local_mods, -1, "Invalid daemon, built with local modifications")

@with_custom_setup( default_setup, basic_teardown )
def test_delete_non_existing() :
    cli = get_client()
    try:
        assert_raises( ArakoonNotFound, cli.delete, "non-existing" )
    except:
        cli._dropConnections()
        raise
    
    set_get_and_delete( cli, "k", "v")
    
        
def sequence_scenario( start_suffix ):
    iter_size = 1000
    cli = get_client()
    
    start_key = key_format_str % start_suffix
    end_key = key_format_str % ( start_suffix + iter_size - 1 )
    seq = arakoon.ArakoonProtocol.Sequence()
    for i in range( iter_size ) :
        seq.addSet( key_format_str % (i+start_suffix), value_format_str % (i+start_suffix) )
    cli.sequence( seq )
    
    key_value_list = cli.range_entries( start_key, True, end_key, True )
    assert_key_value_list(start_suffix, iter_size , key_value_list )
    
    seq = arakoon.ArakoonProtocol.Sequence()
    for i in range( iter_size ) :
        seq.addDelete( key_format_str % (start_suffix + i) )
    cli.sequence( seq )
    
    key_value_list = cli.range_entries( start_key, True, end_key, True )
    assert_equal( len(key_value_list), 0, "Still keys in the store, should have been deleted" )
    
    for i in range( iter_size ) :
        seq.addSet( key_format_str % (start_suffix + i), value_format_str % (start_suffix + i) )
    seq.addDelete( "non-existing" )
    assert_raises( ArakoonNotFound, cli.sequence, seq )
    key_value_list = cli.range_entries( start_key, True, end_key, True )
    assert_equal( len(key_value_list), 0, "There are keys in the store, should not be the case" )
    
    cli._dropConnections()
    
@with_custom_setup( default_setup, basic_teardown )
def test_sequence ():
    sequence_scenario( 10000 )


@with_custom_setup( setup_3_nodes , basic_teardown )   
def test_3_nodes_stop_all_start_slaves ():
    
    key = getRandomString()
    value = getRandomString () 
    
    cli = get_client()
    cli.set(key,value)
    master = cli.whoMaster()
    slaves = filter( lambda node: node != master, node_names )
    
    q.cmdtools.arakoon.stop()
    for slave in slaves:
        q.cmdtools.arakoon.startOne( slave )
    
    cli._dropConnections()
    cli = get_client()
    stored_value = cli.get( key )
    assert_equals( stored_value, value, "Stored value mismatch for key '%s' ('%s' != '%s')" % (key, value, stored_value) )
    
if __name__ == "__main__" :
    from pymonkey import InitBase

    setup_3_nodes_forced_master ()
    q.cmdtools.arakoon.stopOne( "arakoon_1" )
    cli = get_client()    
    cli.set( "key", "value" )


#   test_restart_single_slave_short ()
    teardown()

