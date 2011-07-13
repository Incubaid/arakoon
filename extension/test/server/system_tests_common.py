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


from pymonkey import q, i
from nose.tools import *
from functools import wraps
import traceback
import sys
import subprocess
import signal
import gzip

test_failed = False 

class with_custom_setup ():
    
    def __init__ (self, setup, teardown):
        self.__setup = setup
        self.__teardown = teardown
    
    def __call__ (self, func ):
        @wraps(func)
        def decorate(*args,**kwargs):
            
            global data_base_dir
            data_base_dir = q.system.fs.joinPaths( q.dirs.tmpDir, 'arakoon_system_tests' , func.func_name )
            global test_failed
            test_failed = False
            fatal_ex = None
            home_dir = data_base_dir
            if q.system.fs.exists( data_base_dir):
                q.system.fs.removeDirTree( data_base_dir )
            self.__setup( home_dir )
            try:
                func(*args,**kwargs)
            except Exception, outer :
                tb = traceback.format_exc()
                logging.fatal( tb )              
                fatal_ex = outer
            finally:
                self.__teardown( fatal_ex is None )
            
            if fatal_ex is not None:
                raise fatal_ex
        return decorate


import os
import random
import threading
import time
import arakoon.ArakoonProtocol  
import logging



from arakoon.ArakoonExceptions import * 
 
if __name__ == "__main__" :
    from pymonkey import InitBase

data_base_dir = None
cluster_id = 'sturdy'
node_names = [ "sturdy_0", "sturdy_1", "sturdy_2" ]
node_ips = [ "127.0.0.1", "127.0.0.1", "127.0.0.1"]
node_client_base_port = 7080
node_msg_base_port = 10000
daemon_name = "arakoon"
binary_full_path = "/opt/qbase3/apps/arakoon/bin/arakoon"
lease_duration = 2.0
tlog_entries_per_tlog = 1000

key_format_str = "key_%012d"
value_format_str = "value_%012d"

fs = q.system.fs
proc = q.system.process

def generate_lambda( f, *args, **kwargs ):
    return lambda: f( *args, **kwargs )

def _getCluster():
    global cluster_id 
    return q.manage.arakoon.getCluster(cluster_id)

def dump_tlog (node_id, tlog_number) :
    cluster = _getCluster()
    node_home_dir = cluster.getNodeConfig(node_id ) ['home']
    tlog_full_path =  q.system.fs.joinPaths ( node_home_dir, "%03d.tlog" % tlog_number  )
    cmd = "%s --dump-tlog %s" % (binary_full_path, tlog_full_path)
    logging.debug( "Dumping file %s" % tlog_full_path )
    logging.debug("Command is : '%s'" % cmd )
    (exit,stdout,stderr) = q.system.process.run(cmd)
    assert_equals( exit, 0, "Could not dump tlog for node %s" % node_id )
    return stdout

def get_arakoon_binary() :
    return fs.joinPaths( get_arakoon_bin_dir(), 'arakoon')

def get_arakoon_bin_dir():
    return fs.joinPaths( q.dirs.appDir, "arakoon", "bin")

def get_tcbmgr_path ():
    return fs.joinPaths( get_arakoon_bin_dir(), "tcbmgr" )

def get_diff_path():
    return "/usr/bin/diff"

def get_node_db_file( node_id ) :
    cluster = _getCluster()
    node_home_dir = cluster.getNodeConfig(node_id ) ['home']
    db_file = fs.joinPaths( node_home_dir, node_id + ".db" )
    return db_file
    
def dump_store( node_id ):
    cluster = _getCluster()
    stat = cluster.getStatusOne(node_id )
    msg = "Can only dump the store of a node that is not running (status is %s)" % stat
    assert_equals( stat, q.enumerators.AppStatusType.HALTED, msg)

    db_file = get_node_db_file ( node_id )
    dump_file = db_file + ".dump" 
    cmd = get_tcbmgr_path() + " list -pv " + db_file
    try:
        dump_fd = open( dump_file, 'w' )
        logging.debug( "Dumping store of %s to %s" % (node_id, dump_file) )
        (exit,stdout,stderr) = proc.run( cmd , captureOutput=True, stdout=dump_fd )
        dump_fd.close()
    except:
        logging.info("Unexpected error: %s" % sys.exc_info()[0])

    return dump_file

def compare_stores( node1_id, node2_id ):

    keys_to_skip = [ "*lease", "*i", "*master" ]
    dump1 = dump_store( node1_id )
    dump2 = dump_store( node2_id )
    
    # Line 2 contains the master lease, can be different as it contains a timestamp
    d1_fd = open ( dump1, "r" )
    d2_fd = open ( dump2, "r" )

    cluster = _getCluster()
    def get_i ( node_id ):
        stat = cluster.getStatusOne(node_id )
        assert_equals( stat, q.enumerators.AppStatusType.HALTED, "Can only dump the store of a node that is not running")
        db_file = get_node_db_file(node_id)
        cmd = " ".join( [get_arakoon_binary(), "--dump-store", db_file])
        (exit,stdout,stderr) = proc.run( cmd, captureOutput=True )
        i_line = stdout.split("\n") [0]
        i_str = i_line.split("(")[1][:-1]
        return int(i_str)
     
    

    i1 = get_i (node1_id)
    logging.debug("Counter value for store of %s: %d" % (node1_id,i1))
    i2 = get_i (node2_id)
    logging.debug("Counter value for store of %s: %d" % (node2_id,i2))
    if( abs (i1 - i2) > 1 ):
        logging.error( "Store counters differ too much (%s: %d and %s: %d)" % (node1_id,i1,node2_id,i2) )
    
    i1_line = d1_fd.readline()
    i2_line = d2_fd.readline()
    
    diffs = { node1_id : {} , node2_id : {} }
    
    def get_next_kv ( fd ):
        
        line = fd.readline()
        if line == "" :
            return (None,None)
        parts = line.split("\t")
        if len( parts ) < 2 :
            return get_next_kv( fd )
        else :
            return ( parts[0], "\t".join(parts[1:]))
    
    iter = 0 
    (k1,v1) = get_next_kv( d1_fd )
    (k2,v2) = get_next_kv( d2_fd )
    
    while k1 != None and k2 != None :
        iter+=1
        if( ( k1 == k2 and v1 == v2) ) :
            (k1,v1) = get_next_kv( d1_fd )
            (k2,v2) = get_next_kv( d2_fd )
        else :
            
            if k1 == k2 :
                if k1 not in keys_to_skip:
                    diffs[node1_id][k1] = v1
                    diffs[node2_id][k2] = v2 
                    logging.debug( "Stores have different values for %s" % (k1) )
                (k1,v1) = get_next_kv( d1_fd )
                (k2,v2) = get_next_kv( d2_fd ) 
            if k1 < k2 :
                logging.debug( "Store of %s has a value for, store of %s doesn't" % (node1_id, node2_id) )
                diffs[node1_id][k1] = v1
                (k1,v1) = get_next_kv( d1_fd )
            if k1 > k2 :
                logging.debug( "Store of %s has a value for, store of %s doesn't" % (node2_id, node1_id) )
                diffs[node2_id][k2] = v2
                (k2,v2) = get_next_kv( d2_fd )
    
    if k1 != None :
        logging.debug ( "Store of %s contains more keys, store of %s is EOF" %  (node1_id, node2_id) )
        while k1 != None:
            diffs[node1_id][k1] = v1
            (k1,v1) = get_next_kv( d1_fd )
    if k2 != None:
        logging.debug ( "Store of %s contains more keys, store of %s is EOF" %  (node2_id, node1_id) )
        while k2 != None:
            diffs[node2_id][k2] = v2
            (k2,v2) = get_next_kv ( d2_fd ) 
            
    max_diffs = 0
    
    if ( i1 != i2 ):
        max_diffs = 1
        
    diff_cnt = len( set( diffs[node1_id].keys() ).union( set(diffs[node2_id].keys() ) ) )
    if diff_cnt > max_diffs :
        raise Exception ( "Found too many differences between stores (%d > %d)\n%s" % (diff_cnt, max_diffs,diffs) )

    logging.debug( "Stores of %s and %s are valid" % (node1_id,node2_id))
    return True

def get_tlog_count (node_id ):
    cluster = _getCluster()
    node_home_dir = cluster.getNodeConfig(node_id ) ['home']
    ls = q.system.fs.listFilesInDir
    tlogs =      ls( node_home_dir, filter="*.tlog" )
    tlogs.extend(ls( node_home_dir, filter="*.tlc" ) )
    tlogs.extend(ls( node_home_dir, filter="*.tlf" ) )
    return len(tlogs)
    
def get_last_tlog_id ( node_id ):
    cluster = _getCluster()
    node_home_dir = cluster.getNodeConfig(node_id ) ['home']
    tlog_max_id = 0
    tlog_id = None
    tlogs_for_node = q.system.fs.listFilesInDir( node_home_dir, filter="*.tlog" )
    for tlog in tlogs_for_node:
        tlog = tlog [ len(node_home_dir):]
        tlog = tlog.strip('/')
        tlog_id = tlog.split(".")[0]
        tlog_id = int( tlog_id )
        if tlog_id > tlog_max_id :
            tlog_max_id = tlog_id
    if tlog_id is not None:
        logging.debug("get_last_tlog_id('%s') => %s" % (node_id, tlog_id))
    else :
        raise Exception ("Not a single tlog found in %s" % node_home_dir )
        
    return tlog_max_id
    
def get_last_i_tlog ( node_id ):
    tlog_dump = dump_tlog ( node_id, get_last_tlog_id(node_id) ) 
    tlog_dump_list = tlog_dump.split("\n")
    tlog_first_entry = tlog_dump_list[0]
    tlog_first_i = int(tlog_first_entry.split(":") [0].lstrip(" "))
    if tlog_first_i % tlog_entries_per_tlog != 0 :
        test_failed = True
        raise Exception( "Problem with tlog rollover, first entry (%d) incorrect" % tlog_first_i ) 
    tlog_last_entry = tlog_dump_list [-2]
    tlog_last_i = tlog_last_entry.split(":") [0].lstrip( " 0" )
    return tlog_last_i

def stopOne(name):
    cluster = _getCluster()
    cluster.stopOne(name)

def startOne(name):
    cluster = _getCluster()
    cluster.startOne(name)

def catchupOnly(name):
    cluster = _getCluster()
    cluster.catchupOnly(name)
    
def restart_all():
    cluster = _getCluster()
    cluster.restart()
    
def rotate_logs( max_logs_to_keep = 5, compress_old_files = True):
    for node_name in node_names:
        rotate_log( node_name, max_logs_to_keep, compress_old_files)

def send_signal ( node_name, signal ):
    cluster = _getCluster()
    pid = cluster._getPid(node_name)
    if pid is not None:
        q.system.process.kill( pid, signal )

def rotate_log(node_name, max_logs_to_keep, compress_old_files ):
    cfg = getConfig(node_name)
    log_dir = cfg['log_dir']
    
    log_file = fs.joinPaths(log_dir, "%s.log" % (node_name) )
    if compress_old_files:
        old_log_fmt = fs.joinPaths(log_dir, "%s.log.%%d.gz" % (node_name) )
    else :
        old_log_fmt = fs.joinPaths(log_dir, "%s.log.%%d" % (node_name) )
        
    tmp_log_file = log_file + ".1"
    
    def shift_logs ( ) :
        log_to_remove = old_log_fmt % (max_logs_to_keep - 1) 
        if fs.isFile ( log_to_remove ) :
            fs.unlink(log_to_remove)
            
        for i in range( 1, max_logs_to_keep - 1) :
            j = max_logs_to_keep - 1 - i
            log_to_move = old_log_fmt % j
            new_log_name = old_log_fmt % (j + 1)
            if fs.isFile( log_to_move ) :
                fs.renameFile ( log_to_move, new_log_name )
    cluster = _getCluster()
    shift_logs()
    if fs.isFile( log_file ):
        fs.renameFile ( log_file, tmp_log_file )
        if cluster.getStatusOne(node_name) == q.enumerators.AppStatusType.RUNNING:
            send_signal ( node_name, signal.SIGUSR1 )
        
        if compress_old_files:
            cf = gzip.open( old_log_fmt % 1 , 'w')
            orig = open(tmp_log_file, 'r' )
            cf.writelines(orig)
            cf.close()
            orig.close()
            fs.unlink(tmp_log_file)
    
    
def getConfig(name):
    cluster = _getCluster()
    return cluster.getNodeConfig(name)


def regenerateClientConfig():
    global cluster_id
    clientsCfg = q.config.getConfig("arakoonclients")
    if cluster_id in clientsCfg.keys():
        clusterDir = clientsCfg[cluster_id]["path"]
        clientCfgFile = q.system.fs.joinPaths(clusterDir, "%s_client.cfg" % cluster_id)
        if q.system.fs.exists( clientCfgFile):
            q.system.fs.removeFile( clientCfgFile)
    cliCfg = q.clients.arakoon.getClientConfig( cluster_id )
    cliCfg.generateFromServerConfig()
    

def whipe(name):
    config = getConfig(name)
    data_dir = config['home']
    q.system.fs.removeDirTree(data_dir)
    q.system.fs.createDir(data_dir)
    logging.info("whiped %s" % name)

def get_memory_usage(node_name):
    cluster = _getCluster()
    pid = cluster._getPid(node_name )
    if pid is None:
        return 0
    cmd = "ps -p %s -o vsz" % (pid)
    (exit_code, stdout,stderr) = q.system.process.run( cmd, stopOnError=False)
    if (exit_code != 0 ):
        logging.error( "Coud not determine memory usage: %s" % stderr )
        return 0
    try:
        size_str = stdout.split("\n") [1]
        return int( size_str )
    except Exception as ex:
        logging.error( "Coud not determine memory usage: %s" % ex )
        return 0
    
def collapse(name, n = 1):
    global cluster_id
    config = getConfig(name)
    ip = config['ip']
    port = config['client_port']
    rc = subprocess.call([binary_full_path, '--collapse-remote',cluster_id,ip,port,str(n)])
    return rc

def add_node ( i ):
    ni = node_names[i]
    logging.info( "Adding node %s to config", ni )
    (db_dir,log_dir) = build_node_dir_names(ni)
    cluster = _getCluster()
    cluster.addNode (
        ni,
        node_ips[i], 
        clientPort = node_client_base_port + i,
        messagingPort= node_msg_base_port + i, 
        logDir = log_dir,
        logLevel = 'debug',
        home = db_dir)
    cluster.addLocalNode (ni )
    cluster.createDirs(ni)

def start_all() :
    cluster = _getCluster()
    cluster.start()
    time.sleep(3.0)  

def stop_all():
    logging.info("stop_all")
    cluster = _getCluster()
    cluster.stop()

def restart_all():
    stop_all()
    start_all()

def restart_random_node():
    node_index = random.randint(0, len(node_names) - 1)
    node_name = node_names [node_index ]
    delayed_restart_nodes( [ node_name ] )

def delayed_restart_all_nodes() :
    delayed_restart_nodes( node_names )
    
def delayed_restart_nodes(node_list) :
    downtime = random.random() * 60.0
    for node_name in node_list :
        stopOne(node_name )
    time.sleep( downtime )
    for node_name in node_list :
        startOne(node_name )

def delayed_restart_1st_node ():
    delayed_restart_nodes( [ node_names[0] ] )

def delayed_restart_2nd_node ():
    delayed_restart_nodes( [ node_names[1] ] )

def delayed_restart_3rd_node ():
    delayed_restart_nodes( [ node_names[2] ] )
    
def restart_nodes_wf_sim( n ):
    wf_step_duration = 0.2
    
    for i in range (n):
        stopOne(node_names[i] )
        time.sleep( wf_step_duration )
    
    for i in range (n):    
        startOne(node_names[i] )
        time.sleep( wf_step_duration )

def getRandomString( length = 16 ) :
    def getRC ():
        return chr(random.randint(0,25) + ord('A'))

    retVal = ""
    for i in range( length ) :
        retVal += getRC()        
    return retVal

def build_node_dir_names ( nodeName ):
    global data_base_dir
    data_dir = q.system.fs.joinPaths( data_base_dir, nodeName)
    db_dir = q.system.fs.joinPaths( data_dir, "db")
    log_dir = q.system.fs.joinPaths( data_dir, "log")
    return (db_dir,log_dir)

def setup_n_nodes ( n, force_master, home_dir ):
    global cluster_id
    q.system.process.run( "/sbin/iptables -F" )
    cfg_list = q.config.list()
    
    cluster = q.manage.arakoon.getCluster( cluster_id )
    cluster.tearDown()
        
    logging.info( "Creating data base dir %s" % data_base_dir )
    q.system.fs.createDir ( data_base_dir )
    
    for i in range (n) :
        nodeName = node_names[ i ]
        (db_dir,log_dir) = build_node_dir_names( nodeName )
        cluster = _getCluster()
        cluster.addNode(name=nodeName,
                        clientPort = 7080+i,
                        messagingPort = 10000+i,
                        logDir = log_dir,
                        home = db_dir )
        
        cluster.addLocalNode(nodeName)
        cluster.createDirs(nodeName)

    if force_master:
        logging.info( "Forcing master to %s", node_names[0] )
        cluster.forceMaster(node_names[0] )
    else :
        logging.info( "Using master election" )
        cluster.forceMaster(None )
    
    logging.info( "Creating client config" )
    regenerateClientConfig()
    
    logging.info( "Changing log level to debug for all nodes" )
    cluster.setLogLevel("debug")
    
    lease = int(lease_duration)
    logging.info( "Setting lease expiration to %d" % lease)
    cluster.setMasterLease( lease )
    
    logging.info( "Starting cluster" )
    start_all() 
   
    logging.info( "Setup complete" )
    

def setup_3_nodes_forced_master (home_dir):
    setup_n_nodes( 3, True, home_dir)
    
def setup_2_nodes_forced_master (home_dir):
    setup_n_nodes( 2, True, home_dir)

def setup_1_node_forced_master (home_dir):
    setup_n_nodes( 1, True, home_dir)

def setup_3_nodes (home_dir) :
    setup_n_nodes( 3, False, home_dir)

def setup_2_nodes (home_dir) :
    setup_n_nodes( 2, False, home_dir)
    
def setup_1_node (home_dir):
    setup_n_nodes( 1, False, home_dir )

default_setup = setup_3_nodes

def dummy_teardown():
    pass

def basic_teardown( removeDirs ):
    logging.info("basic_teardown(%s)" % removeDirs)
    global cluster_id
    clusters = q.config.getConfig("arakoonclusters")
    if cluster_id in clusters.keys() :
        logging.info( "Stopping arakoon daemons" )
        stop_all()
    
    for i in range( len(node_names) ):
        destroy_ram_fs( i )

    cluster = _getCluster()
    cluster.tearDown(removeDirs ) 
    if removeDirs:
        q.system.fs.removeDirTree( data_base_dir )
    
    cluster.remove()
    
    logging.info( "Teardown complete" )


def get_client ():
    global cluster_id
    client = q.clients.arakoon.getClient(cluster_id)
    return client


def iterate_n_times (n, f, startSuffix = 0, failure_max=0, valid_exceptions=None ):
    client = get_client ()
    failure_count = 0
    client.recreate = False
    
    if valid_exceptions is None:
        valid_exceptions = []
        
    global test_failed
    
    for i in range ( n ) :
        if test_failed :
            logging.error( "Test marked as failed. Aborting.")
            break
        suffix = ( i + startSuffix )
        key = key_format_str % suffix
        value = value_format_str % suffix
        
        try:
            f(client, key, value )
        except Exception, ex:
            failure_count += 1
            fatal = True
            for valid_ex in valid_exceptions:
                if isinstance(ex, valid_ex ) :
                    fatal = False
            if failure_count > failure_max or fatal :
                client._dropConnections()
                test_failed = True
                logging.critical( "!!! Failing test")
                tb = traceback.format_exc()
                logging.critical( tb )
                raise
        if client.recreate :
            client._dropConnections()
            client = get_client()
            client.recreate = False
            
    client._dropConnections()
        

def create_and_start_thread (f ):
    class MyThread ( threading.Thread ):
        
        def __init__ (self, f, *args, **kwargs ):
            threading.Thread.__init__ ( self )
            self._f = f
            self._args = args
            self._kwargs = kwargs
            
    
        
        def run (self):
            try:
                self._f ( *(self._args), **(self._kwargs) )
            except Exception, ex:
                global test_failed
                logging.critical("!!! Failing test")
                tb = traceback.format_exc()
                logging.critical( tb )
                test_failed = True
                raise
            
    t = MyThread( f )
    t.start ()
    return t
    
    
def create_and_start_thread_list ( f_list ):
    return map ( create_and_start_thread, f_list )
    
def create_and_wait_for_thread_list ( f_list , timeout=None, assert_failure=True ):

    class SyncThread ( threading.Thread ):
        def __init__ (self, thr_list):
            threading.Thread.__init__ ( self )
            self.thr_list = thr_list
            
        def run (self):
            for thr in thr_list :
                thr.join()
    
    global test_failed 
    test_failed = False 
    
    thr_list = create_and_start_thread_list ( f_list )
   
    sync_thr = SyncThread ( thr_list )
    sync_thr.start()
    sync_thr.join( timeout )
    assert_false( sync_thr.isAlive() )
    if assert_failure :
        assert_false( test_failed )
    
    
def create_and_wait_for_threads ( thr_cnt, iter_cnt, f, timeout=None ):
    
    f_list = []
    for i in range( thr_cnt ) :
        g = lambda : iterate_n_times(iter_cnt, f )
        f_list.append( g )
    
    create_and_wait_for_thread_list( f_list, timeout)    
    
def mindless_simple_set( client, key, value):
    try:
        client.set( key, value)
    except Exception, ex:
        logging.info( "Error while setting => %s: %s" , ex.__class__.__name__, ex)

def simple_set(client, key, value):
    client.set( key, value )

def assert_get( client, key, value):
    assert_equals( client.get(key), value )

def set_get_and_delete( client, key, value):
    client.set( key, value )
    assert_equals( client.get(key), value )
    try:
        client.delete( key )
    except ArakoonNotFound as ex:
        logging.debug( "Caught ArakoonNotFound on delete. Ignoring" )
    assert_raises ( ArakoonNotFound, client.get, key )

def mindless_retrying_set_get_and_delete( client, key, value ):
    def validate_ex ( ex, tryCnt ):
        return True
    
    generic_retrying_set_get_and_delete( client, key, value, validate_ex )
    
    
def generic_retrying_set_get_and_delete( client, key, value, is_valid_ex ):
    start = time.time()
    failed = True
    tryCnt = 0
    
    global test_failed
    
    last_ex = None 
    
    while ( failed and time.time() < start + 5.0 ) :
        try :
            tryCnt += 1
            client.set( key,value )
            assert_equals( client.get(key), value )
            try:
                client.delete( key )
            except ArakoonNotFound:
                logging.debug("Master switch while deleting key")
            # assert_raises ( ArakoonNotFound, client.get, key )
            failed = False
            last_ex = None
        except (ArakoonNoMaster, ArakoonNodeNotMaster), ex:
            if isinstance(ex, ArakoonNoMaster) :
                logging.debug("No master in cluster. Recreating client.")
            else :
                logging.debug("Old master is not yet ready to succumb. Recreating client")
            
            # Make sure we propagate the need to recreate the client 
            # (or the next iteration we are back to using the old one)
            client.recreate = True
            client._dropConnections()
            client = get_client() 
            
        except Exception, ex:
            logging.debug( "Caught an exception => %s: %s", ex.__class__.__name__, ex )
            time.sleep( 0.5 )
            last_ex = ex
            if not is_valid_ex( ex, tryCnt ) :
                # test_failed = True
                logging.debug( "Re-raising exception => %s: %s", ex.__class__.__name__, ex )
                raise
    
    if last_ex is not None:
        raise last_ex

    
def retrying_set_get_and_delete( client, key, value ):
    def validate_ex ( ex, tryCnt ):
        ex_msg = "%s" % ex
        validEx = False
        
        validEx = validEx or isinstance( ex, ArakoonSockNotReadable )
        validEx = validEx or isinstance( ex, ArakoonSockReadNoBytes )
        validEx = validEx or isinstance( ex, ArakoonSockRecvError )
        validEx = validEx or isinstance( ex, ArakoonSockRecvClosed )
        validEx = validEx or isinstance( ex, ArakoonSockSendError )
        validEx = validEx or isinstance( ex, ArakoonNotConnected ) 
        validEx = validEx or isinstance( ex, ArakoonNodeNotMaster )
        if validEx:
            logging.debug( "Ignoring exception: %s", ex_msg )
        return validEx
    
    generic_retrying_set_get_and_delete( client, key, value, validate_ex) 
    
def add_node_scenario ( node_to_add_index ):
    iterate_n_times( 100, simple_set )
    
    stop_all()
    
    add_node( node_to_add_index )
    global cluster_id
    sn = '%s_nodes' % cluster_id
    if cluster_id in q.config.list():
        q.config.remove(sn)
    
    regenerateClientConfig()
    start_all()

    iterate_n_times( 100, assert_get )
    
    iterate_n_times( 100, set_get_and_delete, 100)

def assert_key_value_list( start_suffix, list_size, list ):
    assert_equals( len(list), list_size )
    for i in range( list_size ) :
        suffix = start_suffix + i
        key = key_format_str % (suffix )
        value = value_format_str % (suffix )
        assert_equals ( (key,value) , list [i] )

def assert_last_i_in_sync ( node_1, node_2 ):
    last_i_0 = get_last_i_tlog( node_1 )
    last_i_1 = get_last_i_tlog( node_2 )    
    abs_diff = abs(int(last_i_0) - int(last_i_1) )
    assert_equals( max(abs_diff,1) , 1, 
                   "Values for i are invalid %s %s" % (last_i_0, last_i_1) )  

def assert_running_nodes ( n ):
    assert_not_equals ( q.system.process.checkProcess( daemon_name, n), 1, "Number of expected running nodes missmatch" )

def assert_value_list ( start_suffix, list_size, list ) :
    assert_list( value_format_str, start_suffix, list_size, list )

def assert_key_list ( start_suffix, list_size, list ) :
    assert_list( key_format_str, start_suffix, list_size, list )
 
def assert_list ( format_str, start_suffix, list_size, list ) :
    assert_equals( len(list), list_size )
    
    for i in range( list_size ) :
        elem = format_str % (start_suffix + i)
        assert_equals ( elem , list [i] )

def run_cmd (cmd, display_output = True) :
    q.system.process.execute( cmd, outputToStdout = display_output )
    
def dir_to_fs_file_name (dir_name):
    return dir_name.replace( "/", "_")

def destroy_ram_fs( node_index ) :
    (mount_target,log_dir) = build_node_dir_names( node_names[node_index] ) 
    
    try :
        cmd = "umount %s" % mount_target
        run_cmd ( cmd )
    except :
        pass
    
def delayed_master_restart_loop ( iter_cnt, delay ) :
    for i in range( iter_cnt ):
        global test_failed
        try:
            time.sleep( delay )
            cli = get_client()
            cli.set('delayed_master_restart_loop','delayed_master_restart_loop')
            master_id = cli.whoMaster()
            cli._dropConnections()
            stopOne( master_id )
            startOne( master_id )
        except:
            logging.critical("!!!! Failing test. Exception in restart loop.")
            test_failed = True
            raise
                     
def restart_loop( node_index, iter_cnt, int_start_stop, int_stop_start ) :
    for i in range (iter_cnt) :
        node = node_names[node_index]
        time.sleep( 1.0 * int_start_stop )
        stopOne(node)
        time.sleep( 1.0 * int_stop_start )
        startOne(node)
        

def restart_single_slave_scenario( restart_cnt, set_cnt ) :
    start_stop_wait = 3.0
    stop_start_wait = 1.0
    slave_loop = lambda : restart_loop( 1, restart_cnt, start_stop_wait, stop_start_wait )
    set_loop = lambda : iterate_n_times( set_cnt, set_get_and_delete )
    create_and_wait_for_thread_list( [slave_loop, set_loop] )
    
    # Give the slave some time to catch up 
    time.sleep( 5.0 )
    stop_all()
    assert_last_i_in_sync ( node_names[0], node_names[1] )
    compare_stores( node_names[0], node_names[1] )

def get_entries_per_tlog():
    cmd = "%s --version" % binary_full_path 
    (exit,stdout,stderr) = q.system.process.run(cmd)
    assert_equals( exit, 0 )
    return int(stdout.split('\n')[-2].split(':')[1])

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
