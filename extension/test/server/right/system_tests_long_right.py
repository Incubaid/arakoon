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
from threading import Thread, Condition


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
    cli.set('','')
    assert_true( cli.expectProgressPossible(),
                 "Master store counter is ahead of slave" )

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
        time.sleep(1.0)
        Common.assert_running_nodes ( 2 )
        if cli.expectProgressPossible() :
            break
        iter_cnt += 1

    Common.flush_stores ()
    Common.stop_all()
    Common.assert_last_i_in_sync( node_names[0], node_names[1])
    Common.compare_stores( node_names[0], node_names[1] )
    pass

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

@Common.with_custom_setup( Common.setup_2_nodes_forced_master_normal_slaves , Common.basic_teardown )
def test_missed_accept ():


    # Give the new node some time to recognize the master
    time.sleep(0.5)
    node_names = Common.node_names
    zero = node_names[0]
    one = node_names[1]
    Common.flush_store( one )
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
    Common.flush_store( zero )
    Common.stop_all()
    Common.assert_last_i_in_sync(zero, one )
    Common.compare_stores( zero, one )

@Common.with_custom_setup( Common.setup_2_nodes_forced_master, Common.basic_teardown)
def test_is_progress_possible_witness_node():
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

    # forced slaves should be caught up very fast
    counter = 0
    max_wait = 10
    up2date = False

    while not up2date and counter < max_wait :
        time.sleep( 1.0 )
        counter += 1
        up2date = cli.expectProgressPossible()

    if counter >= max_wait :
        raise Exception ("Node did not catchup in a timely fashion")

    cli.set('k','v')


@Common.with_custom_setup( Common.setup_2_nodes_forced_master_normal_slaves, Common.basic_teardown)
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


def drop_master(n):
    cli = Common.get_client()
    previousMaster = cli.whoMaster()
    for i in range(n):
        logging.info("starting iteration %i", i)
        Common.dropMaster(previousMaster)
        cli._masterId = None
        master = cli.whoMaster()
        assert_not_equals(master, previousMaster, "Master did not change after drop master request.")
        previousMaster = master
        logging.info("finished iteration %i", i)

@Common.with_custom_setup( Common.setup_3_nodes, Common.basic_teardown)
def test_drop_master():
    Common.stopOne( Common.node_names[0] )
    n = 20
    drop_master(n)
"""    drop_masters = lambda : drop_master(n)
    Common.create_and_wait_for_threads ( 1, 1, drop_masters, n*8*3 )"""


def _test_drop_master_with_load_(client):
    global busy, excs

    Common.stopOne( Common.node_names[0] )
    n = 40

    busy = True
    excs = []
    cv = Condition()

    t0 = Thread(target = lambda : client(0))
    t0.start()
    t1 = Thread(target = lambda : client(1))
    t1.start()
    t2 = Thread(target = lambda : client(2))
    t2.start()
    t3 = Thread(target = lambda : client(3))
    t3.start()

    def inner_drop_master():
        global busy
        try :
            drop_master(n)
            cv.acquire()
            busy = False
            cv.notify()
            cv.release()
        except Exception, ex:
            excs.append(ex)
            cv.acquire()
            busy = False
            cv.notify()
            cv.release()
            raise ex


    t_drop = Thread(target = inner_drop_master)
    t_drop.start()

    cv.acquire()
    cv.wait(900.0)

    assert_false( busy ) # test should be finished by now and is probably hanging
    if busy:
        busy = False
        time.sleep(10) # give the client threads some time to finish
        assert_false( True ) # test should be finished by now and is probably hanging

    time.sleep(10) # give the client threads some time to finish
    if len(excs) <> 0:
        raise excs[0]

@Common.with_custom_setup( Common.setup_3_nodes, Common.basic_teardown)
def test_drop_master_with_load():
    def client(n):
        global excs, busy
        try :
            while busy:
                Common.iterate_n_times( 100, Common.retrying_set_get_and_delete, startSuffix = n * 100 )
        except Exception, ex:
            excs.append(ex)
            raise ex

    _test_drop_master_with_load_(client)

@Common.with_custom_setup( Common.setup_3_nodes_witness_slave, Common.basic_teardown)
def test_drop_master_witness_slave():
    Common.stopOne( Common.node_names[0] )
    cli = Common.get_client()
    cli.nop()
    master = cli.whoMaster()
    Common.dropMaster(master)
    cli.nop()

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



@Common.with_custom_setup(Common.setup_3_nodes_mini, Common.basic_teardown )
def test_disable_tlog_compression():
    """
    assert we can disable tlog compression (eta: 25s)
    """
    clu = _getCluster()
    clu.disableTlogCompression()
    clu.restart()
    time.sleep(1.0)

    tlog_size = 1000 # mini

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

@Common.with_custom_setup(Common.default_setup, Common.basic_teardown)
def test_fsync():
    c = _getCluster()
    cli = Common.get_client()
    cli.set('k1', 'v')
    c.enableFsync()
    c.restart()
    cli.get('k1')
    cli.set('k2', 'v')
    c.disableFsync()
    c.restart()
    cli.get('k2')
    cli.set('k3', 'v')
    assert_equals(cli.deletePrefix('k'), 3)

@Common.with_custom_setup(Common.setup_1_node_mini, Common.basic_teardown)
def test_sabotage():
    """
    scenario countering a sysadmin removing files (s)he shouldn't (eta : 16s)
    """
    clu = _getCluster()
    tlog_size = 1000
    num_tlogs = 2
    test_size = num_tlogs * tlog_size
    Common.iterate_n_times(test_size, Common.simple_set)
    time.sleep(10)
    clu.stop()
    node_id = Common.node_names[0]
    node_cfg = clu.getNodeConfig(node_id)
    node_home_dir = node_cfg ['home']
    node_tlf_dir = node_cfg ['tlf_dir']
    q = Common.q
    files = map(lambda x : "%s/%s" % (node_home_dir, x),
                [ "002.tlog",
                  "%s.db" % (node_id,),
                  #"%s.db.wal" % (node_id,), # should not exist after a `proper` close
                  ])
    for f in files:
        print f
        q.system.fs.remove(f)

    cmd = clu._cmd('sturdy_0')
    returncode = subprocess.call(cmd)
    assert_equals(returncode, 50)

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
def test_db_optimize_witness_node():
    assert_raises( Exception, Common.optimizeDb, Common.node_names[0] )
    assert_raises( Exception, Common.optimizeDb, Common.node_names[1] )

@Common.with_custom_setup( Common.setup_3_nodes_forced_master_normal_slaves,
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

@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_missing_tlog():
    Common.iterate_n_times(550000,Common.simple_set)
    nn = Common.node_names[0]
    Common.stopOne(nn)
    fn = Common.get_node_db_file(nn)
    os.remove(fn)
    logging.info("removed %s", fn)
    cluster = _getCluster()
    fs = Common.q.system.fs
    cfg = cluster.getNodeConfig(nn)
    node_tlf_dir  = cfg['tlf_dir']
    tlx_full_path = fs.joinPaths (node_tlf_dir, "002.tlf")
    logging.info("removing %s", tlx_full_path)
    os.remove(tlx_full_path)


    Common.startOne(nn)
    # it should die some time later.
    def n_running () :
        ns = Common.check_output(['pgrep', '-c', Common.daemon_name])
        n = int(ns)
        return n

    t0 = time.time()
    wait = True
    n = 0
    while wait:
        time.sleep(10)
        try:
            n = n_running()
        except:
            n = 0

        if n == 0:
            wait = False

        t1 = time.time()
        d = t1 - t0
        logging.info("after d=%fs, the node's still running", d)
        if d > 120.0:
            wait = False


    ok_(n == 0,"node still running")
    #now check logging.
    log_dir = cfg['log_dir']
    log_file = fs.joinPaths(log_dir, "%s.log" % nn)



    size = os.path.getsize(log_file)
    f = open(log_file,'r')
    f.seek(size - 4096)
    tail = f.read()
    f.close()
    ok = False
    for line in tail.split('\n'):
        if line.find("(found neither 002.tls nor 002.tlog)"):
            logging.info("line=%s",line)
            ok = True

    ok_(ok, "line should be present")
