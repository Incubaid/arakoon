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

def _getCluster():
    q = Common.q # resistance is futile
    return q.manage.arakoon.getCluster(Common.cluster_id)

@Common.with_custom_setup( Common.default_setup, Common.basic_teardown )
def test_single_client_100000_sets():
    Common.iterate_n_times( 100000, Common.simple_set )

@Common.with_custom_setup( Common.setup_3_nodes_forced_master, Common.basic_teardown )
def test_delete_non_existing_with_catchup ():
    """
    catchup after deleting a non existing value (eta: 6s)
    """
    Common.stopOne( Common.node_names[1] )
    key='key'
    value='value'
    cli = Common.get_client()
    try:
        cli.delete( key )
    except:
        pass
    cli.set(key,value)
    cli.set(key,value)
    cli.set(key,value)
    
    slave = Common.node_names[1]
    Common.startOne( slave )
    time.sleep(2.0)
    cluster = _getCluster()
    log_dir = cluster.getNodeConfig(slave ) ['log_dir']
    log_file = "%s/%s.log" % (log_dir, slave)
    q = Common.q 
    log = q.system.fs.fileGetContents( log_file )
    assert_equals( log.find( "don't fit" ), -1, "Store counter out of sync" )
    
@Common.with_custom_setup(Common.setup_2_nodes_forced_master, Common.basic_teardown )
def test_expect_progress_fixed_master ():
    """
    see if expect_progress gives the correct result after a cluster restart (eta: 67s)
    """
    Common.stopOne( Common.node_names[1] )
    key='key'
    value='value'
    cli = Common.get_client()
    try:
        cli.set(key,value)
    except:
        pass
    Common.restart_all()
    time.sleep(1.0)
    assert_true( cli.expectProgressPossible(),
                 "Master store counter is ahead of slave" )
    
@Common.with_custom_setup( Common.setup_3_nodes_forced_master, Common.basic_teardown )
def test_restart_single_slave_long ():
    Common.restart_single_slave_scenario( 100, 10000 )

@Common.with_custom_setup( Common.default_setup, Common.basic_teardown )
def test_20_clients_1000_sets() :
    arakoon.ArakoonProtocol.ARA_CFG_TIMEOUT = 60.0
    Common.create_and_wait_for_threads ( 20, 1000, Common.simple_set, 200.0 )

@Common.with_custom_setup( Common.setup_3_nodes, Common.basic_teardown)
def test_tlog_rollover():
    Common.iterate_n_times( 150000, Common.simple_set )
    Common.stop_all()
    Common.start_all()
    Common.iterate_n_times( 150000, Common.simple_set )

@Common.with_custom_setup( Common.setup_2_nodes, Common.basic_teardown)
def test_catchup_while_collapsing():
    node_names = Common.node_names
    tpet = Common.tlog_entries_per_tlog
    Common.iterate_n_times(2* tpet, Common.simple_set )
    
    Common.stop_all()
    Common.wipe( node_names[0] )
    Common.startOne(node_names[1])
    
    delayed_start = lambda: Common.startOne(node_names[0])
    collapser = lambda: Common.collapse(node_names[1] )
    
    Common.create_and_wait_for_thread_list( [delayed_start, collapser] )
    cli = Common.get_client()
    
    time_out = 120
    iter_cnt = 0
    
    while iter_cnt < time_out :
        Common.assert_running_nodes ( 2 )
        if cli.expectProgressPossible() :
            break
        iter_cnt += 1
        time.sleep(1.0)
        
    Common.stop_all()
    Common.assert_last_i_in_sync( node_names[0], node_names[1])
    Common.compare_stores( node_names[0], node_names[1] )
    pass   

@Common.with_custom_setup( Common.default_setup, Common.basic_teardown )
def test_restart_master_long ():
    restart_iter_cnt = 10
    def write_loop ():
        Common.iterate_n_times( 100000, 
                                Common.retrying_set_get_and_delete, 
                                failure_max=2*restart_iter_cnt, 
                                valid_exceptions=[ArakoonSockNotReadable,
                                                  ArakoonNotFound,
                                                  ArakoonGoingDown
                                                  ] )
        
    def restart_loop (): 
        Common.delayed_master_restart_loop( restart_iter_cnt , 
                                            1.5 * Common.lease_duration )
    global test_failed
    test_failed = False 
    Common.create_and_wait_for_thread_list( [restart_loop, write_loop] )

    cli = Common.get_client()
    time.sleep(2.0)
    key = "key"
    value = "value"
    cli.set(key, value)
    set_value = cli.get(key)
    assert_equals(  value, set_value , 
        "Key '%s' does not have expected value ('%s' iso '%s')" % (key, set_value, value) )
    
    Common.stop_all()
    Common.start_all()
    Common.stop_all()
    node_names = Common.node_names

    Common.assert_last_i_in_sync( node_names[0], node_names[1] )
    Common.assert_last_i_in_sync( node_names[2], node_names[1] )
    Common.compare_stores( node_names[0], node_names[1] )
    Common.compare_stores( node_names[2], node_names[1] )
    cli.dropConnections()
    logging.info("end of `test_restart_master_long`")
    
@Common.with_custom_setup( Common.default_setup, Common.basic_teardown ) 
def test_master_reelect():
    cli = Common.get_client() 
    master_id = cli.whoMaster()
    assert_not_equals ( master_id, None, "No master to begin with. Aborting.")
    
    key = "k"
    value = "v"
    cli.set(key ,value )

    logging.info("stopping master:%s", master_id)
    Common.stopOne( master_id )
    ld = Common.lease_duration

    delay = 1.5 * ld
    time.sleep(delay)
    logging.info("waited %s, for reelection to happen" % delay)
    logging.info("config=%s" % (cli._config))
    
    cli._masterId = None
    
    new_master_id = cli.whoMaster()
    assert_not_equals ( new_master_id,
                        None,
                        "No new master elected, no master. Aborting.")
    assert_not_equals ( new_master_id,
                        master_id,
                        "No new master elected, same master. Aborting.")
    
    assert_equals( cli.get(key), value)
    Common.startOne( master_id )
    
    # Give old master some time to catch up
    time.sleep( 5.0 )
    
    Common.stopOne ( new_master_id )
    
    time.sleep( 2.0 * ld )
    
    cli = Common.get_client()
    newest_master_id = cli.whoMaster()
    assert_not_equals ( newest_master_id,
                        None,
                        "No new master elected, no master. Aborting.")
    assert_not_equals ( newest_master_id,
                        new_master_id,
                        "No new master elected, same master. Aborting.")


@Common.with_custom_setup( Common.setup_3_nodes, Common.basic_teardown)
def test_large_tlog_collection_restart():
    
    Common.iterate_n_times( 100002, Common.simple_set )
    Common.stop_all()
    Common.start_all()
    Common.iterate_n_times( 100, Common.set_get_and_delete )
    

@Common.with_custom_setup( Common.setup_3_nodes, Common.basic_teardown )
def test_3_node_stop_master_slaves_restart():
    
    logging.info( "starting test case")
    Common.iterate_n_times( 1000, Common.simple_set )
    cli = Common.get_client()
    master = cli.whoMaster()
    slaves = filter( lambda node: node != master, Common.node_names[:3] )
    Common.stopOne( master )
    ld = Common.lease_duration
    logging.info (ld )
    nap_time = 2 * ld
    logging.info( "Stopped master. Sleeping for %0.2f secs" % nap_time )
    
    print nap_time
    
    time.sleep( nap_time )
    logging.info( "Stopping old slaves")
    for node in slaves:
        print "Stopping %s" % node
        Common.stopOne( node )
    
    logging.info( "Starting old master")
    Common.startOne( master )
    time.sleep(0.2)
    
    logging.info( "Starting old slaves")
    for node in slaves:
        Common.startOne( node )
    
    cli.dropConnections()
    cli = Common.get_client()
    
    logging.info( "Sleeping a while" )
    time.sleep( ld / 2 )
    
    Common.iterate_n_times( 1000, Common.set_get_and_delete )
    cli.dropConnections()

@Common.with_custom_setup( Common.setup_2_nodes_forced_master , Common.basic_teardown )
def test_missed_accept ():
    
    
    # Give the new node some time to recognize the master 
    time.sleep(0.5)
    node_names = Common.node_names
    zero = node_names[0]
    one = node_names[1]
    Common.stopOne(one)
    
    cli = Common.get_client()
    try:
        cli.set("k","v")
    except Exception, ex:
        logging.info( "Caught exception (%s: '%s'" , ex.__class__.__name__, ex )

    Common.startOne (one)
    # Give the node some time to catch up
    time.sleep( 1.0 )
    
    Common.iterate_n_times( 1000, Common.set_get_and_delete )
    time.sleep(1.0)
    Common.stop_all()
    Common.assert_last_i_in_sync(zero, one )
    Common.compare_stores( zero, one )

@Common.with_custom_setup( Common.setup_2_nodes_forced_master, Common.basic_teardown)
def test_is_progress_possible():
    time.sleep(0.2)
    def write_loop (): 
        Common.iterate_n_times( 48000, 
                                Common.retrying_set_get_and_delete  )
    logging.info("before write loop")
    Common.create_and_wait_for_thread_list( [write_loop] )
   
    logging.info( "Stored all keys" ) 
    Common.stop_all()

    Common.wipe(Common.node_names[1])

    cli = Common.get_client()
    Common.start_all()
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
    cl = _getCluster()
    cl.setLogLevel("debug")
    
    Common.regenerateClientConfig(Common.cluster_id)
            
    Common.restart_nodes_wf_sim( 1 )
    n1 = Common.node_names[1]
    logging.info("going to start %s", n1)
    Common.startOne(n1 )
    
    Common.create_and_wait_for_thread_list ( [ large_write_loop ] )
    
    Common.add_node( 2 )
    cl = _getCluster()
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
    
    
@Common.with_custom_setup( Common.setup_3_nodes, Common.basic_teardown )
def test_3_nodes_2_slaves_down ():
    """ make sure the 'set' operation fails when 2 slaves are down, (eta: 63s) """
    cli = Common.get_client()
    master_id = cli.whoMaster()
    
    slaves = filter( lambda n: n != master_id, Common.node_names[:3] )
    for slave in slaves:
        Common.stopOne( slave )
    
    assert_raises( ArakoonSockNotReadable, cli.set, 'k', 'v' )
            
    cli.dropConnections()



@Common.with_custom_setup( Common.default_setup, Common.basic_teardown )
def test_disable_tlog_compression():
    """
    assert we can disable tlog compression (eta: 460s)
    """
    clu = _getCluster()
    clu.disableTlogCompression()
    clu.restart()
    time.sleep(1.0)
    
    tlog_size = Common.get_entries_per_tlog() 
    
    num_tlogs = 2
    test_size = num_tlogs*tlog_size
    Common.iterate_n_times(test_size, Common.simple_set )
    
    logging.info("Tlog_size: %d", tlog_size)
    node_id = Common.node_names[0]
    node_home_dir = clu.getNodeConfig(node_id) ['home']
    q = Common.q
    ls = q.system.fs.listFilesInDir
    time.sleep(2.0)
    tlogs = ls(node_home_dir, filter="*.tlog" )
    expected = num_tlogs + 1 
    tlog_len = len(tlogs)
    assert_equals(tlog_len, expected, 
                  "Wrong number of uncompressed tlogs (%d != %d)" % (expected, tlog_len)) 
 

@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_sabotage():
    """
    scenario countering a sysadmin removing files (s)he shouldn't (eta : 400s)
    """
    clu = _getCluster()
    tlog_size = Common.get_entries_per_tlog()
    num_tlogs = 2
    test_size = num_tlogs * tlog_size + 20
    Common.iterate_n_times(test_size, Common.simple_set)
    time.sleep(10)
    clu.stop()
    node_id = Common.node_names[0]
    node_home_dir = clu.getNodeConfig(node_id) ['home']
    q = Common.q
    files = map(lambda x : "%s/%s" % (node_home_dir, x),
                [ "002.tlog",
                  "%s.db" % (node_id,),
                  #"%s.db.wal" % (node_id,), # should not exist after a `proper` close
                  ])
    for f in files:
        print f
        q.system.fs.remove(f)
    clu.start()
    delay = 80
    logging.info("sleeping for %i", delay)
    time.sleep(delay)
    logging.info("doing 2000 sets")
    Common.iterate_n_times(2000, Common.simple_set)
    logging.info("sleeping fo 10s")
    time.sleep(10)
    
    size = q.system.fs.fileSize("%s/001.tlf" % node_home_dir)
    logging.info("file_size = %i", size)
    assert_true(size > 1024 * 5)

@Common.with_custom_setup( Common.setup_3_nodes_forced_master, Common.basic_teardown )
def test_large_catchup_while_running():
    """ make sure catchup does not interphere with normal operation (eta : 720s) """
    cli = Common.get_client()
    cluster = Common._getCluster()

    cli.set('k','v')
    m = cli.whoMaster()

    nod1 = Common.node_names[0]
    nod2 = Common.node_names[1]
    nod3 = Common.node_names[2]

    n_name,others = (nod1, [nod2,nod3]) if nod1 != m else (nod2, [nod1, nod3])
    node_pid = cluster._getPid(n_name)

    time.sleep(0.1)
    Common.q.system.process.run( "kill -STOP %s" % str(node_pid) )
    Common.iterate_n_times( 200000, Common.simple_set )
    for n in others:
        Common.collapse(n)

    time.sleep(1.0)
    Common.q.system.process.run( "kill -CONT %s" % str(node_pid) )
    cli.delete('k')
    time.sleep(10.0)
    Common.assert_running_nodes(3)


@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_log_rotation():
    node = Common.node_names[0]
    for i in range(100):
        Common.rotate_log(node, 1, False)
        time.sleep(0.2)
        Common.assert_running_nodes(1)
    
@Common.with_custom_setup(Common.setup_3_nodes_mini, Common.basic_teardown)
def test_243():
    node_names = Common.node_names
    zero = node_names[0]
    one = node_names[1]
    two = node_names[2]
    npt = 1000
    n = 5 * npt + 200

    logging.info("%i entries per tlog", npt)
    logging.info("doing %i sets", n)
    Common.iterate_n_times(n, Common.simple_set)


    logging.info("did %i sets, now collapse all ", n)
    Common.collapse(zero,1)
    Common.collapse(one,1)
    Common.collapse(two,1)

    logging.info("set %i more", n)
    Common.iterate_n_times(n, Common.simple_set)

    logging.info("stopping %s", zero)
    Common.stopOne(zero)
    logging.info("... and set %s more ",n)
    Common.iterate_n_times(n, Common.simple_set)

    client = Common.get_client()
    stats = client.statistics()
    node_is = stats['node_is']
    logging.info("node_is=%s",node_is)
    logging.info("collapse 2 live nodes")
    Common.collapse(one,1)
    Common.collapse(two,1)
    Common.startOne(zero)

    stats = client.statistics ()
    node_is = stats['node_is']
    mark = max(node_is.values())
    catchup = True
    t0 = time.time()
    timeout = False
    logging.info("wait until catchup is done ....")
    count = 0

    while catchup:
        time.sleep(10)
        stats = client.statistics()
        node_is = stats['node_is']
        logging.info("node_is=%s", node_is)
        lowest = min(node_is.values())
        if lowest >= mark or count == 10:
            catchup = False
        count = count + 1

    stats = client.statistics()
    node_is = stats['node_is']
    logging.info("done waiting")
    logging.info("OK,node_is=%s", node_is)
    logging.info("now collapse %s", zero)
    rc = Common.collapse(zero,1)
    # if it does not throw, we should be ok.
    if rc :
        msg = "rc = %s; should be 0" % rc
        raise Exception(msg)
    logging.info("done")

@Common.with_custom_setup( Common.setup_3_nodes_forced_master, Common.basic_teardown )
def test_large_catchup_while_running():
    """
    test_large_catchup_while_running : assert cluster is still ok after (catchup // collapse) (eta : 600s)
    """
    cli = Common.get_client()
    cluster = Common._getCluster()

    cli.set('k','v')
    m = cli.whoMaster()

    nod1 = Common.node_names[0]
    nod2 = Common.node_names[1]
    nod3 = Common.node_names[2]

    n_name,others = (nod1, [nod2,nod3]) if nod1 != m else (nod2, [nod1, nod3])
    node_pid = cluster._getPid(n_name)

    time.sleep(0.1)
    Common.q.system.process.run( "kill -STOP %s" % str(node_pid) )
    Common.iterate_n_times( 200000, Common.simple_set )
    for n in others:
        Common.collapse(n)

    time.sleep(1.0)
    Common.q.system.process.run( "kill -CONT %s" % str(node_pid) )
    cli.delete('k')
    time.sleep(10.0)
    Common.assert_running_nodes(3)


@Common.with_custom_setup( Common.setup_3_nodes_forced_master, 
                           Common.basic_teardown)
def test_db_optimize():
    """
    test_db_optimize : asserts an 'optimizeDb' call shrinks the database enough (eta: 100s)
    """
    assert_raises( Exception, Common.optimizeDb, Common.node_names[0] )
    Common.iterate_n_times(10000, Common.set_get_and_delete)
    db_file = Common.get_node_db_file( Common.node_names[1] )
    start_size = os.path.getsize( db_file )
    Common.optimizeDb(Common.node_names[1]) 
    opt_size = os.path.getsize(db_file)
    template = "Size did not shrink (enough). Original: '%d'. Optimized: '%d'." 
    msg = template % (start_size, opt_size) 
    assert_true( opt_size < 0.1*start_size, msg)

@Common.with_custom_setup (Common.setup_3_nodes_forced_master,
                           Common.basic_teardown)
def test_db_defrag():
    """
    test_db_defrag : asserts the defrag call works, and actually shrinks the database (eta: 650s)
    """
    assert_raises( Exception, Common.defragDb, Common.node_names[0] )
    client = Common.get_client()
    a = 16807
    m = 2147483647
    seed = 1
    q = m / a
    r = m % a
    for i in xrange(100 * 1000):
        hi = seed / q
        lo = seed % q
        test = a * lo - r * hi
        if test > 0:
            seed = test
        else:
            seed = test + m
        key = seed
        key_s = "key_%09i" % key
        vs = random.randint(113, 257)
        v = "xxxxxxxxxx" * vs
        client.set(key_s,key_s)
        client.set(key_s,v)
        print i, key_s, vs
        if i == 65537:
            seed = 1

    slave = Common.node_names[1]
    print slave
    db_file = Common.get_node_db_file( slave)
    start_size = os.path.getsize( db_file )
    print "start_size=", start_size
    Common.defragDb(Common.node_names[1]) 
    opt_size = os.path.getsize(db_file)
    template = "Size did not shrink (enough). Original: '%d'. Optimized: '%d'." 
    msg = template % (start_size, opt_size) 
    assert_true( opt_size < 0.9 * start_size, msg)



@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_272():
    """ 
    test_272 : arakoon can go down during log rotation, but you need to have load to reproduce it (eta: 1110s)
    """
    node = Common.node_names[0]
    cluster = Common._getCluster()
    path = '%s.cfg' % cluster._getConfigFilePath()
    logging.info('path=%s', path)
    bench = subprocess.Popen([Common.binary_full_path, '-config', path ,'--benchmark'])
    time.sleep(10.0) # give it time to get up to speed
    rc = bench.returncode
    if rc <> None:
        raise Exception ("benchmark should not have finished yet.")

    for i in range(100):
        Common.rotate_log(node, 1, False)
        time.sleep(0.2)
        # Note:
        # At this point, we expect one node to be running, as well as the
        # benchmark process. The `assert_running_nodes` procedure uses
        # 'pgrep -c' which counts the number of 'arakoon' processes running, so
        # we should pass *2* here, despite only a single node process should be
        # running.
        Common.assert_running_nodes(2)
        rc = bench.returncode
        if rc <> None:
            raise Exception ("benchmark should not have stopped")

    Common.assert_running_nodes(2)
    logging.info("now wait for benchmark to finish")
    rc = bench.wait()
    if rc <> 0:
        raise Exception("benchmark exited with rc = %s" % rc)

@Common.with_custom_setup(Common.setup_3_nodes_mini, Common.basic_teardown)
def test_inject_as_head():
    """
    test_inject_as_head : asserts shortcut for those who don't want to collapse periodically. (eta: 20 s)
    ARAKOON-288
    ARAKOON-308
    """
    cluster = Common._getCluster()
    npt = 1000
    n = 4 * npt + 65
    Common.iterate_n_times(n,Common.simple_set)
    client = Common.get_client()
    m = client.whoMaster()
    slaves = filter(lambda x:x != m, Common.node_names)
    s0 = slaves[0]
    s1 = slaves[1]
    new_head = '/tmp/test_inject_as_head.db'
    print "1"
    cluster.backupDb(s0, new_head)
    logging.info("backup-ed %s from %s", new_head, s0)
    cluster.injectAsHead(s1, new_head)
    print "2"
    logging.info("injected as head")
    Common.iterate_n_times(n,Common.simple_set)
    logging.info("iterated")
    print "3"
    cluster.remoteCollapse(s1, 3)
    logging.info("done")
    print "4"
    ntlogs = Common.get_tlog_count(s1)
    logging.info("get_tlog_dir => %i", ntlogs)
    
    
    
