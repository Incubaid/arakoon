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
import system_tests_common
import logging 

@with_custom_setup( default_setup, basic_teardown )
def test_single_client_100000_sets():
    iterate_n_times( 100000, simple_set )

@with_custom_setup( setup_3_nodes_forced_master, basic_teardown )
def test_delete_non_existing_with_catchup ():
    q.cmdtools.arakoon.stopOne( node_names[1] )
    key='key'
    value='value'
    cli = get_client()
    try:
        cli.delete( key )
    except:
        pass
    cli.set(key,value)
    cli.set(key,value)
    cli.set(key,value)
    
    slave = node_names[1]
    q.cmdtools.arakoon.startOne( slave )
    time.sleep(2.0)
    log_dir = q.config.arakoon.getNodeConfig( slave ) ['log_dir']
    log_file = q.system.fs.joinPaths( log_dir, '%s.log' % slave )
    log = q.system.fs.fileGetContents( log_file )
    assert_equals( log.find( "don't fit" ), -1, "Store counter out of sync" )
    
@with_custom_setup( setup_2_nodes_forced_master, basic_teardown )
def test_expect_progress_fixed_master ():
    q.cmdtools.arakoon.stopOne( node_names[1] )
    key='key'
    value='value'
    cli = get_client()
    try:
        cli.set(key,value)
    except:
        pass
    q.cmdtools.arakoon.restart()
    time.sleep(1.0)
    assert_true( cli.expectProgressPossible(), "Master store counter is ahead of slave" )
    
        
@with_custom_setup( setup_3_nodes_forced_master, basic_teardown )
def test_restart_single_slave_long ():
    restart_single_slave_scenario( 100, 10000 )

@with_custom_setup( default_setup, basic_teardown )
def test_20_clients_1000_sets() :
    arakoon.ArakoonProtocol.ARA_CFG_TIMEOUT = 60.0
    create_and_wait_for_threads ( 20, 1000, simple_set, 100.0 )

@with_custom_setup( setup_3_nodes, basic_teardown)
def test_tlog_rollover():
    iterate_n_times( 150000, simple_set )
    q.cmdtools.arakoon.stop()
    start_all()
    iterate_n_times( 150000, simple_set )

@with_custom_setup( default_setup, basic_teardown )
def test_restart_master_long ():
    restart_iter_cnt = 10
    write_loop = lambda: iterate_n_times( 100000, retrying_set_get_and_delete, failure_max=2*restart_iter_cnt, valid_exceptions=[ArakoonSockNotReadable] )
    restart_loop = lambda: delayed_master_restart_loop( restart_iter_cnt , 1.5*lease_duration )
    create_and_wait_for_thread_list( [restart_loop, write_loop] )
    
    cli = get_client()
    time.sleep(2.0)
    key = "key"
    value = "value"
    cli.set(key, value)
    set_value = cli.get(key)
    assert_equals(  value, set_value , 
        "Key '%s' does not have expected value ('%s' iso '%s')" % (key, set_value, value) )
    
    q.cmdtools.arakoon.stop()
    start_all()
    q.cmdtools.arakoon.stop()
    
    assert_last_i_in_sync( node_names[0], node_names[1] )
    assert_last_i_in_sync( node_names[2], node_names[1] )
    
    cli._dropConnections()
    
@with_custom_setup( default_setup, basic_teardown ) 
def test_master_reelect():
    cli = get_client() 
    master_id = cli.whoMaster()
    assert_not_equals ( master_id, None, "No master to begin with. Aborting.")
    
    key = "k"
    value = "v"
    cli.set(key ,value )
    
    q.cmdtools.arakoon.stopOne( master_id )
    
    time.sleep( 1.5 * lease_duration )
    
    cli._masterId = None
    new_master_id = cli.whoMaster()
    assert_not_equals ( new_master_id, None, "No new master elected, no master. Aborting.")
    assert_not_equals ( new_master_id, master_id, "No new master elected, same master. Aborting.")
    
    assert_equals( cli.get(key), value)
    q.cmdtools.arakoon.startOne( master_id )
    
    # Give old master some time to catch up
    time.sleep( 5.0 )
    
    q.cmdtools.arakoon.stopOne ( new_master_id )
    
    time.sleep( 1.5 * lease_duration )
    
    cli._masterId = None
    newest_master_id = cli.whoMaster()
    assert_not_equals ( newest_master_id, None, "No new master elected, no master. Aborting.")
    assert_not_equals ( newest_master_id, new_master_id, "No new master elected, same master. Aborting.")


@with_custom_setup( setup_3_nodes, basic_teardown)
def test_large_tlog_collection_restart():
    
    iterate_n_times( 100002, simple_set )
    q.cmdtools.arakoon.stop()
    start_all()
    iterate_n_times( 100, set_get_and_delete )
    

@with_custom_setup( setup_3_nodes, basic_teardown )
def test_3_node_stop_master_slaves_restart():
    
    logging.info( "starting test case")
    iterate_n_times( 1000, simple_set )
    cli = get_client()
    master = cli.whoMaster()
    slaves = filter( lambda node: node != master, node_names )
    q.cmdtools.arakoon.stopOne( master )
    logging.info ( lease_duration )
    nap_time = 2 * lease_duration
    logging.info( "Stopped master. Sleeping for %0.2f secs" % nap_time )
    
    print nap_time
    
    time.sleep( nap_time )
    logging.info( "Stopping old slaves")
    for node in slaves:
        print "Stopping %s" % node
        q.cmdtools.arakoon.stopOne( node )
    
    logging.info( "Starting old master")
    q.cmdtools.arakoon.startOne( master )
    time.sleep(0.2)
    
    logging.info( "Starting old slaves")
    for node in slaves:
        q.cmdtools.arakoon.startOne( node )
    
    cli._dropConnections()
    cli = get_client()
    
    logging.info( "Sleeping a while" )
    time.sleep( lease_duration / 2 )
    
    iterate_n_times( 1000, set_get_and_delete )
    cli._dropConnections()

@with_custom_setup( setup_2_nodes_forced_master , basic_teardown )
def test_missed_accept ():
    
    
    # Give the new node some time to recognize the master 
    time.sleep(0.5)
    q.cmdtools.arakoon.stopOne( node_names[1] )
    
    cli = get_client()
    try:
        cli.set("k","v")
    except Exception, ex:
        logging.info( "Caught exception (%s: '%s'" , ex.__class__.__name__, ex )

    q.cmdtools.arakoon.startOne ( node_names[1] )
    # Give the node some time to catch up
    time.sleep( 1.0 )
    
    iterate_n_times( 1000, set_get_and_delete )
    
    assert_last_i_in_sync( node_names[0], node_names[1] )
    
@with_custom_setup( setup_2_nodes_forced_master, basic_teardown)
def test_is_progress_possible():
    time.sleep(0.2)
    write_loop = lambda: iterate_n_times( 50000, retrying_set_get_and_delete  )
    create_and_wait_for_thread_list( [write_loop] )
   
    logging.info( "Stored all keys" ) 
    q.cmdtools.arakoon.stop()
    
    slave_config = q.config.arakoon.getNodeConfig( node_names[1] )
    data_dir = slave_config['home']
    q.system.fs.removeDirTree( data_dir )
    q.system.fs.createDir ( data_dir )
    logging.info( "Slave wiped" )
 
    cli = get_client()
    q.cmdtools.arakoon.start()
    logging.info( "nodes started" )
    assert_false( cli.expectProgressPossible() )
    
    counter = 0
    max_wait = 60*5
    up2date = False
    
    while not up2date and counter < max_wait :
        time.sleep( 1.0 )
        counter += 1
        up2date = cli.expectProgressPossible()
    
    if counter >= max_wait :
        raise Exception ("Node did not catchup in a timely fashion")
    
    cli.set('k','v')
    
@with_custom_setup( setup_1_node_forced_master, basic_teardown )
def test_sso_deployment():
    
    system_tests_common.test_failed = False
    
    write_loop = lambda: iterate_n_times( 10000, retrying_set_get_and_delete )
    large_write_loop = lambda: iterate_n_times( 280000, retrying_set_get_and_delete, startSuffix = 1000000 ) 
    write_thr1 = create_and_start_thread ( write_loop )
    non_retrying_write_loop = lambda: iterate_n_times( 10000, set_get_and_delete, startSuffix = 2000000  )
    
    add_node( 1 )
    config = q.config.getInifile("arakoon")
    config.setParam(node_names[1],"log_level","debug")
    config.write()
        
    if "arakoonnodes" in q.config.list():
        q.config.remove("arakoonnodes")   
    q.config.arakoonnodes.generateClientConfigFromServerConfig()
            
    restart_nodes_wf_sim( 1 )
    q.cmdtools.arakoon.startOne( node_names[1] )
    
    write_thr2 = create_and_wait_for_thread_list ( [ large_write_loop ] )
    
    add_node( 2 )
    config = q.config.getInifile("arakoon")
    config.setParam(node_names[2],"log_level","debug")
    config.write()
    
    q.config.arakoon.forceMaster( None )
    
    if "arakoonnodes" in q.config.list():
        q.config.remove("arakoonnodes")   

    q.config.arakoonnodes.generateClientConfigFromServerConfig()
    
    restart_nodes_wf_sim( 2 )
    q.cmdtools.arakoon.startOne( node_names[2] )
    time.sleep( 0.3 )
    assert_running_nodes ( 3 )
    
    write_thr3 = create_and_start_thread ( non_retrying_write_loop )

    write_thr1.join()
    write_thr2.join()
    write_thr3.join()
    
    assert_false ( system_tests_common.test_failed )
    
@with_custom_setup( setup_3_nodes, basic_teardown )
def test_3_nodes_2_slaves_down ():
    
    cli = get_client()
    master_id = cli.whoMaster()
    
    slaves = filter( lambda n: n != master_id, node_names )
    for slave in slaves:
        q.cmdtools.arakoon.stopOne( slave )
    
    assert_raises( ArakoonSockNotReadable, cli.set, 'k', 'v' )
            
    cli._dropConnections()
