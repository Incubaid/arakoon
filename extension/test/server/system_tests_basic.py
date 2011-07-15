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

from system_tests_common import *
import logging
import arakoon.ArakoonProtocol

@with_custom_setup ( setup_1_node_forced_master, basic_teardown )
def test_start_stop_single_node_forced () :
    global cluster_id
    assert_running_nodes ( 1 )
    cluster = q.manage.arakoon.getCluster(cluster_id)
    cluster.stop() 
    assert_running_nodes ( 0 )
    cluster.start() 
    assert_running_nodes ( 1 )

@with_custom_setup ( setup_3_nodes_forced_master, basic_teardown )
def test_start_stop_three_nodes_forced () :
    global cluster_id
    cluster = q.manage.arakoon.getCluster(cluster_id)
    assert_running_nodes ( 3 )
    cluster.stop() 
    assert_running_nodes ( 0 )
    cluster.start() 
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


@with_custom_setup(setup_1_node_forced_master, basic_teardown)
def test_assert():
    client = get_client()
    client.set ('test_assert','test_assert')
    client.aSSert('test_assert', 'test_assert')    
    try:
        client.aSSert('test_assert','something_else')
        raise Exception('this should not happen')
    except ArakoonException as inst:
        logging.info('inst=%s',inst)

    seq = arakoon.ArakoonProtocol.Sequence()
    seq.addAssert('test_assert','test_assert')
    seq.addSet('test_assert','changed')
    client.sequence( seq )
    v = client.get('test_assert')

    assert_equals(v, 'changed', 'first_sequence')

    seq2 = arakoon.ArakoonProtocol.Sequence() 
    seq2.addAssert('test_assert','test_assert')
    seq2.addSet('test_assert','changed2')
    try:
        client.sequence(seq2)
    except ArakoonException as inst:
        pass
    v = client.get('test_assert')
    assert_equals(v, 'changed', 'second_sequence: %s <> %s' % (v,'changed'))    

@with_custom_setup ( setup_1_node_forced_master, basic_teardown )
def test_large_value ():
    value = 'x' * (10 * 1024 * 1024)
    client = get_client()
    try:
        client.set ('some_key', value)
        raise Exception('this should have failed')
    except ArakoonException as inst:
        logging.info('inst=%s', inst)
    
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

@with_custom_setup(default_setup, basic_teardown)
def test_aSSert_scenario_1():
    client = get_client()
    client.set('x','x')
    try:
        client.aSSert('x','x') 
    except ArakoonException as ex:
        logging.error ( "Bad stuff happened: %s" % ex)
        assert_equals(True,False)

@with_custom_setup(default_setup, basic_teardown)
def test_aSSert_scenario_2():
    client = get_client()
    client.set('x','x')
    assert_raises( ArakoonAssertionFailed, client.aSSert, 'x', None)

@with_custom_setup(default_setup, basic_teardown)
def test_aSSert_scenario_3():
    client = get_client()
    client.set('x','x')
    ass = arakoon.ArakoonProtocol.Assert('x','x')
    seq = arakoon.ArakoonProtocol.Sequence()
    seq.addUpdate(ass)
    client.sequence(seq)
        
    
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
        
        try:
            client.delete( key )
        except ArakoonNotFound:
            logging.error ( "Caught not found for key %s" % key )
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
    try :
        cli.delete( 'non-existing' )
    except ArakoonNotFound as ex:
        ex_msg = "%s" % ex
        assert_equals( "'non-existing'", ex_msg, "Delete did not return the key, got: %s" % ex_msg)
    set_get_and_delete( cli, "k", "v")


@with_custom_setup( default_setup, basic_teardown )
def test_delete_non_existing_sequence() :
    cli = get_client()
    seq = arakoon.ArakoonProtocol.Sequence()
    seq.addDelete( 'non-existing' )
    try :
        cli.sequence( seq )
    except ArakoonNotFound as ex:
        ex_msg = "%s" % ex
        assert_equals( "'non-existing'", ex_msg, "Sequence did not return the key, got: %s" % ex_msg)
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
    
    stop_all()
    for slave in slaves:
        startOne( slave )
    
    cli._dropConnections()
    cli = get_client()
    stored_value = cli.get( key )
    assert_equals( stored_value, value, "Stored value mismatch for key '%s' ('%s' != '%s')" % (key, value, stored_value) )
    
@with_custom_setup( default_setup, basic_teardown )
def test_get_storage_utilization():
    cl = q.manage.arakoon.getCluster( cluster_id )
    cli = get_client()
    cli.set('key','value')
    time.sleep(0.2)
    stop_all()
    
    def get_total_size( d ):
        assert_not_equals( d['log'], 0, "Log dir cannot be empty")
        assert_not_equals( d['db'], 0 , "Db dir cannot be empty")
        assert_not_equals( d['tlog'], 0, "Tlog dir cannot be empty")
        return d['log'] + d['tlog'] + d['db'] 
    
    logging.debug("Testing global utilization")
    d = cl.getStorageUtilization()
    total = get_total_size(d)
    logging.debug("Testing node 0 utilization")
    d = cl.getStorageUtilization( node_names[0] )
    n1 = get_total_size(d)
    logging.debug("Testing node 1 utilization")
    d = cl.getStorageUtilization( node_names[1] )
    n2 = get_total_size(d)
    logging.debug("Testing node 2 utilization")
    d = cl.getStorageUtilization( node_names[2] )
    n3 = get_total_size(d)
    sum = n1+n2+n3
    assert_equals(sum, total, "Sum of storage size per node (%d) should be same as total (%d)" % (sum,total))


@with_custom_setup( default_setup, basic_teardown )
def test_gather_evidence():
    
    import system_tests_common
    data_base_dir = system_tests_common.data_base_dir
    
    cluster = q.manage.arakoon.getCluster(cluster_id)
    dest_file = q.system.fs.joinPaths( data_base_dir, "evidence.tgz" )
    dest_dir = q.system.fs.joinPaths( data_base_dir, "evidence" )
    destination = 'file://%s' % dest_file 
    cluster.gatherEvidence(destination, test = True)
    
    q.system.fs.targzUncompress(dest_file, dest_dir)
    nodes = q.system.fs.listDirsInDir(dest_dir)
    files = []
    for node in nodes:
        files.extend(q.system.fs.listFilesInDir(node))
    assert_equals(len(nodes),3, "Did not get data from all nodes")
    assert_equals(len(files) > 12, True , "Did not gather enough files")

@with_custom_setup( default_setup, basic_teardown ) 
def test_get_key_count():
    cli = get_client()
    c = cli.getKeyCount()
    assert_equals(c, 0, "getKeyCount should return 0 but got %d" % c)
    test_size = 100
    iterate_n_times( test_size, simple_set )
    c = cli.getKeyCount()
    assert_equals(c, test_size, "getKeyCount should return %d but got %d" % (test_size, c) )


@with_custom_setup (setup_2_nodes, basic_teardown )
def test_download_db():
    iterate_n_times( 100, simple_set )
    cli = get_client()
    m = cli.whoMaster()
    global cluster_id
    clu = q.manage.arakoon.getCluster(cluster_id)
    assert_raises(Exception, clu.backupDb, m, "/tmp/backup")
    stop_all()
    
    n0 = node_names[0]
    n1 = node_names[1]
    startOne(n0)
    time.sleep(0.1)
    db_file = get_node_db_file (n1)
    clu.backupDb(n0, db_file)
    assert_running_nodes(1)
    stop_all()
    compare_stores(n0,n1)